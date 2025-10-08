import os
from dotenv import load_dotenv
from dataclasses import dataclass
import time
import json
import csv
import requests
import random
import threading

SESSION = requests.Session()
DEFAULT_TIMEOUT = (5, 10) 
MAX_BATCH = 200
MAX_RETRIES = 5
BASE_BACKOFF = 0.5  # seconds
SHUTTING_DOWN = threading.Event()

load_dotenv()

@dataclass
class Config:
    open_weather_api_key: str
    weather_api_key: str
    csv_file: str
    source_type: str
    logz_token: str
    logz_listener: str
    logz_type: str
    polling_interval: int
    cities: list[str]

    @staticmethod
    def load():
        return Config(
            open_weather_api_key=os.getenv("OPEN_WEATHER_API_KEY"),
            weather_api_key=os.getenv("WEATHER_API_KEY"),
            csv_file=os.getenv("CSV_FILE"),
            source_type=os.getenv("SOURCE_TYPE"),
            logz_token=os.getenv("LOGZ_TOKEN"),
            logz_listener=os.getenv("LOGZ_LISTENER"),
            logz_type=os.getenv("LOGZ_TYPE"),
            polling_interval=int(os.getenv("POLLING_INTERVAL", "60")),
            cities=[c.strip() for c in os.getenv("CITIES", "").split(",")]
        )


def normalize_open_weather(raw):
    return {
        "city": raw.get("name"),
        "temperature_celsius": raw.get("main", {}).get("temp"),
        "description": raw.get("weather", [{}])[0].get("description"),
        "source_provider": "open_weather",
    }

def normalize_weather_api(raw):
    return {
        "city": raw.get("location", {}).get("name"),
        "temperature_celsius": raw.get("current", {}).get("temp_c"),
        "description": raw.get("current", {}).get("condition", {}).get("text"),
        "source_provider": "weather_api",
    }

def backoff(i, retry_after) -> float:
    if retry_after is not None:
        return retry_after
    return (BASE_BACKOFF * (2 ** i)) + random.random() * 0.5

def send_to_logz_io(batch_logs, config):
    if not batch_logs:
        return
    
    url = f"https://listener.logz.io:8071/{config.logz_listener}?token={config.logz_token}"
    headers = {
        "Content-Type": "application/json",
    }

    i = 0

    while True:
        try:
            response = SESSION.post(url, data=batch_logs, headers=headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if SHUTTING_DOWN.is_set():
                raise
            if e.response is not None and e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after is not None:
                    time.sleep(backoff(i, float(retry_after)))
                i += 1
                continue
            raise

def fetch_open_weather(city, config):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={config.open_weather_api_key}&units=metric"
    i = 0
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if SHUTTING_DOWN.is_set():
                raise
            if e.response is not None and e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after is not None:
                    time.sleep(backoff(i, float(retry_after)))
                i += 1
                continue
            raise

def fetch_weather_api(city, config):
    url = f"https://api.weatherapi.com/v1/current.json?key={config.weather_api_key}&q={city}"
    i = 0
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if SHUTTING_DOWN.is_set():
                raise
            if e.response is not None and e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after is not None:
                    time.sleep(backoff(i, float(retry_after)))
                i += 1
                continue
            raise

def read_csv_file(csv_file, logs):
    with open(csv_file, newline="", encoding="latin1") as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            city = row["city"]
            temperature_celsius = float(row["temperature"])
            description = row["description"].strip('"')
            source_provider = "csv file"
            normalized_file_data = {
                "city": city,
                "temperature_celsius": temperature_celsius,
                "description": description,
                "source_provider": source_provider
            }
            logs.append(normalized_file_data)

def to_ndjson(logs):
    return "\n".join(json.dumps(log) for log in logs) + "\n"

def main():
    config = Config.load()
    count = 1

    while True:
        logs = []

        if config.source_type == "FILE":
            read_csv_file(config.csv_file, logs)

        if config.source_type == "OPEN_WEATHER":
            for city in config.cities:
                raw_open_weather = fetch_open_weather(city, config)
                normalized_open_weather = normalize_open_weather(raw_open_weather)
                logs.append(normalized_open_weather)

        if config.source_type == "WEATHER_API":
            for city in config.cities:
                raw_weather_api = fetch_weather_api(city, config)
                normalized_weather_api = normalize_weather_api(raw_weather_api)
                logs.append(normalized_weather_api)
        
        send_to_logz_io(to_ndjson(logs), config)

        print("Polling", count, "sent to Logz.io successfully")
        count += 1

        time.sleep(config.polling_interval)


if __name__ == "__main__":
    main()