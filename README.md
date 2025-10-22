# Multi-Source-Data-Shipper

Fetch weather from CSV/OpenWeather/WeatherAPI for many cities and ship logs to Logz.io.

## Install

```bash
pip install python-dotenv requests pytest aiohttp python-dotenv
```

## Setup

Create a `.env` file (edit values):

```env
SOURCE_TYPES=FILE,OPEN_WEATHER,WEATHER_API
CSV_FILE=weather.csv
CITIES=Berlin,Sydney
OPEN_WEATHER_API_KEY=your_key
WEATHER_API_KEY=your_key
LOGZ_LISTENER=listener.logz.io
LOGZ_TOKEN=your_token
POLLING_INTERVAL=60
CONCURRENCY=20
```

## Run

```bash
python main.py
```

## Test (data transforms)

```bash
pytest -q
```
