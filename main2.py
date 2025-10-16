#!/usr/bin/env python3
import os, json, csv, gzip, io, asyncio, random, signal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from abc import ABC, abstractmethod

import aiohttp
from dotenv import load_dotenv

# ------------ Config ------------

load_dotenv()

@dataclass
class Config:
    open_weather_api_key: Optional[str]
    weather_api_key: Optional[str]
    csv_file: Optional[str]
    source_types: List[str]         # e.g. ["FILE","OPEN_WEATHER","WEATHER_API"]
    logz_token: str
    logz_listener: str              # e.g. listener.logz.io:8071 or listener.logz.io
    logz_type: str                  # not used in body, but you could add it as a field
    polling_interval: int
    cities: List[str]
    concurrency: int                # max concurrent HTTP calls

    @staticmethod
    def load() -> "Config":
        def get_list(name: str) -> List[str]:
            return [x.strip() for x in os.getenv(name, "").split(",") if x.strip()]

        listener = os.getenv("LOGZ_LISTENER", "listener.logz.io")
        # allow either host or host:port; default port is 8071
        if ":" not in listener:
            listener = f"{listener}:8071"

        return Config(
            open_weather_api_key=os.getenv("OPEN_WEATHER_API_KEY"),
            weather_api_key=os.getenv("WEATHER_API_KEY"),
            csv_file=os.getenv("CSV_FILE"),
            source_types=[s.strip().upper() for s in os.getenv("SOURCE_TYPES", os.getenv("SOURCE_TYPE","")).split(",") if s.strip()],
            logz_token=os.getenv("LOGZ_TOKEN", ""),
            logz_listener=listener,
            logz_type=os.getenv("LOGZ_TYPE", "weather"),
            polling_interval=int(os.getenv("POLLING_INTERVAL", "60")),
            cities=get_list("CITIES"),
            concurrency=int(os.getenv("CONCURRENCY", "20")),
        )

# ------------ Utilities ------------

BASE_BACKOFF = 0.5  # seconds
MAX_RETRIES = 5
NDJSON_GZIP_THRESHOLD = 64_000  # bytes (pre-compress string length)

def backoff(attempt: int, retry_after_s: Optional[float]) -> float:
    if retry_after_s is not None:
        return retry_after_s
    # exponential backoff with jitter
    return (BASE_BACKOFF * (2 ** attempt)) + random.random() * 0.5

def to_ndjson_bytes(events: Sequence[Dict[str, Any]]) -> Tuple[bytes, Dict[str, str]]:
    """
    Returns (body_bytes, headers) with appropriate Content-Type and optional Content-Encoding.
    """
    if not events:
        return b"", {"Content-Type": "application/x-ndjson"}

    s = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    if len(s) > NDJSON_GZIP_THRESHOLD:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(s.encode("utf-8"))
        return buf.getvalue(), {
            "Content-Type": "application/x-ndjson",
            "Content-Encoding": "gzip",
        }
    return s.encode("utf-8"), {"Content-Type": "application/x-ndjson"}

# ------------ DataSource interface + impls ------------

class DataSource(ABC):
    """Unified interface each provider/file implements."""
    name: str

    @abstractmethod
    async def fetch_many(self, cities: Sequence[str]) -> List[Dict[str, Any]]:
        """Return normalized records (dicts) for the requested cities."""
        ...

# -- CSV file source
class CsvSource(DataSource):
    name = "csv_file"

    def __init__(self, path: str):
        self.path = path

    async def fetch_many(self, cities: Sequence[str]) -> List[Dict[str, Any]]:
        # CPU/IO-bound, but tiny; run in default loop executor for non-blocking
        def _read() -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            if not self.path or not os.path.exists(self.path):
                return out
            with open(self.path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=",")
                for row in reader:
                    out.append({
                        "city": row.get("city"),
                        "temperature_celsius": float(row["temperature"]) if row.get("temperature") else None,
                        "description": (row.get("description") or "").strip('"'),
                        "source_provider": self.name,
                    })
            return out
        return await asyncio.to_thread(_read)

# -- OpenWeather source
class OpenWeatherSource(DataSource):
    name = "open_weather"
    def __init__(self, api_key: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
        self.api_key = api_key
        self.session = session
        self.sem = sem

    def _normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "city": raw.get("name"),
            "temperature_celsius": (raw.get("main") or {}).get("temp"),
            "description": (raw.get("weather") or [{}])[0].get("description"),
            "source_provider": self.name,
        }

    async def _fetch_one(self, city: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {"q": city, "appid": self.api_key, "units": "metric"}
        attempt = 0
        while True:
            async with self.sem:
                try:
                    async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status in (429, 500, 502, 503, 504, 408):
                            if attempt >= MAX_RETRIES:
                                return None
                            retry_after = r.headers.get("Retry-After")
                            retry_after_s = float(retry_after) if (retry_after and retry_after.isdigit()) else None
                            await asyncio.sleep(backoff(attempt, retry_after_s))
                            attempt += 1
                            continue
                        r.raise_for_status()
                        data = await r.json()
                        return self._normalize(data)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt >= MAX_RETRIES:
                        return None
                    await asyncio.sleep(backoff(attempt, None))
                    attempt += 1

    async def fetch_many(self, cities: Sequence[str]) -> List[Dict[str, Any]]:
        tasks = [asyncio.create_task(self._fetch_one(c)) for c in cities]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [r for r in results if r is not None]

# -- WeatherAPI source
class WeatherApiSource(DataSource):
    name = "weather_api"
    def __init__(self, api_key: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
        self.api_key = api_key
        self.session = session
        self.sem = sem

    def _normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        cur = (raw.get("current") or {})
        cond = (cur.get("condition") or {})
        return {
            "city": (raw.get("location") or {}).get("name"),
            "temperature_celsius": cur.get("temp_c"),
            "description": cond.get("text"),
            "source_provider": self.name,
        }

    async def _fetch_one(self, city: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        url = "https://api.weatherapi.com/v1/current.json"
        params = {"key": self.api_key, "q": city}
        attempt = 0
        while True:
            async with self.sem:
                try:
                    async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status in (429, 500, 502, 503, 504, 408):
                            if attempt >= MAX_RETRIES:
                                return None
                            retry_after = r.headers.get("Retry-After")
                            retry_after_s = float(retry_after) if (retry_after and retry_after.isdigit()) else None
                            await asyncio.sleep(backoff(attempt, retry_after_s))
                            attempt += 1
                            continue
                        r.raise_for_status()
                        data = await r.json()
                        return self._normalize(data)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt >= MAX_RETRIES:
                        return None
                    await asyncio.sleep(backoff(attempt, None))
                    attempt += 1

    async def fetch_many(self, cities: Sequence[str]) -> List[Dict[str, Any]]:
        tasks = [asyncio.create_task(self._fetch_one(c)) for c in cities]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [r for r in results if r is not None]

# ------------ Source registry / factory ------------

async def build_sources(cfg: Config, session: aiohttp.ClientSession) -> List[DataSource]:
    sem = asyncio.Semaphore(cfg.concurrency)
    sources: List[DataSource] = []
    for kind in cfg.source_types:
        if kind == "FILE" and cfg.csv_file:
            sources.append(CsvSource(cfg.csv_file))
        elif kind == "OPEN_WEATHER":
            sources.append(OpenWeatherSource(cfg.open_weather_api_key or "", session, sem))
        elif kind == "WEATHER_API":
            sources.append(WeatherApiSource(cfg.weather_api_key or "", session, sem))
        # else: ignore unknown or missing keys
    return sources

# ------------ Shipping (async) ------------

async def send_to_logz_io(events: Sequence[Dict[str, Any]], cfg: Config, session: aiohttp.ClientSession) -> None:
    if not events:
        return
    body, headers = to_ndjson_bytes(events)
    url = f"https://{cfg.logz_listener}/?token={cfg.logz_token}"
    attempt = 0
    while True:
        try:
            async with session.post(url, data=body, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 413 and len(events) > 1:
                    mid = len(events) // 2
                    await send_to_logz_io(events[:mid], cfg, session)
                    await send_to_logz_io(events[mid:], cfg, session)
                    return
                if r.status in (429, 500, 502, 503, 504, 408):
                    if attempt >= MAX_RETRIES:
                        r.raise_for_status()
                    retry_after = r.headers.get("Retry-After")
                    retry_after_s = float(retry_after) if (retry_after and retry_after.isdigit()) else None
                    await asyncio.sleep(backoff(attempt, retry_after_s))
                    attempt += 1
                    continue
                r.raise_for_status()
                return
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt >= MAX_RETRIES:
                raise
            await asyncio.sleep(backoff(attempt, None))
            attempt += 1

# ------------ App loop ------------

async def poll_once(cfg: Config, sources: List[DataSource]) -> List[Dict[str, Any]]:
    """Fetch from all configured sources concurrently; return aggregated normalized events."""
    # Run all sources in parallel
    results_per_source = await asyncio.gather(*(s.fetch_many(cfg.cities) for s in sources))
    # Flatten
    events: List[Dict[str, Any]] = []
    for batch in results_per_source:
        events.extend(batch)
    return events

async def main_async():
    cfg = Config.load()
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=0, enable_cleanup_closed=True)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sources = await build_sources(cfg, session)

        stop = asyncio.Event()

        def _handle_signal(*_):
            stop.set()

        # Graceful shutdown on SIGINT/SIGTERM (SIGTERM not on Windows shells; Ctrl+C works)
        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, _handle_signal)
            if hasattr(signal, "SIGTERM"):
                loop.add_signal_handler(signal.SIGTERM, _handle_signal)
        except NotImplementedError:
            pass  # signals not available (e.g., on Windows event loop)

        count = 1
        while not stop.is_set():
            events = await poll_once(cfg, sources)
            await send_to_logz_io(events, cfg, session)
            print(f"Polling {count} sent to Logz.io successfully (events={len(events)})")
            count += 1
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.polling_interval)
            except asyncio.TimeoutError:
                pass  # loop again

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
