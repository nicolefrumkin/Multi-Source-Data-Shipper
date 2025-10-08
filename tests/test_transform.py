import json
import pytest
from main import normalize_open_weather, normalize_weather_api

def test_normalize_open_weather():
    raw = {
        "name": "Berlin",
        "main": {"temp": 18.5},
        "weather": [{"description": "Scattered clouds"}],
    }
    expected = {
        "city": "Berlin",
        "temperature_celsius": 18.5,
        "description": "Scattered clouds",
        "source_provider": "open_weather"
    }
    assert normalize_open_weather(raw) == expected

def test_normalize_open_weather_empty():
    raw = {}
    expected = {
        "city": None,
        "temperature_celsius": None,
        "description": None,
        "source_provider": "open_weather"
    }
    assert normalize_open_weather(raw) == expected

def test_normalize_weather_api():
    raw = {
        "location": {"name": "Berlin"},
        "current": {"temp_c": 18.5, "condition": {"text": "Scattered clouds"}},
    }
    expected = {
        "city": "Berlin",
        "temperature_celsius": 18.5,
        "description": "Scattered clouds",
        "source_provider": "weather_api"
    }
    assert normalize_weather_api(raw) == expected

def test_normalize_weather_api_empty():
    raw = {}
    expected = {
        "city": None,
        "temperature_celsius": None,
        "description": None,
        "source_provider": "weather_api"
    }
    assert normalize_weather_api(raw) == expected



