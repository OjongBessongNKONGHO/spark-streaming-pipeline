"""
Unit tests for fetch.py.
All API calls are mocked so no real network requests are made.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from producer.fetch import fetch_weather, fetch_all_cities


MOCK_API_RESPONSE = {
    "sys": {"country": "FR"},
    "main": {
        "temp": 23.5,
        "feels_like": 22.1,
        "humidity": 65,
        "pressure": 1013,
    },
    "wind": {"speed": 5.2, "deg": 180},
    "visibility": 10000,
    "weather": [{"main": "Clear", "description": "clear sky"}],
    "clouds": {"all": 10},
    "dt": 1700000000,
}

MOCK_CITY = {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522}


def test_fetch_weather_success():
    """Successful API response should return a clean weather dictionary."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_API_RESPONSE
    mock_response.raise_for_status.return_value = None

    with patch("producer.fetch.requests.get", return_value=mock_response):
        result = fetch_weather(MOCK_CITY)

    assert result is not None
    assert result["city"] == "Paris"
    assert result["temperature"] == 23.5
    assert result["humidity"] == 65
    assert result["weather_description"] == "clear sky"
    assert result["latitude"] == 48.8566


def test_fetch_weather_returns_correct_fields():
    """Returned dictionary should contain all required fields."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_API_RESPONSE
    mock_response.raise_for_status.return_value = None

    with patch("producer.fetch.requests.get", return_value=mock_response):
        result = fetch_weather(MOCK_CITY)

    required_fields = [
        "city",
        "country",
        "temperature",
        "feels_like",
        "humidity",
        "pressure",
        "wind_speed",
        "wind_direction",
        "visibility",
        "weather_condition",
        "weather_description",
        "cloudiness",
        "recorded_at",
        "latitude",
        "longitude",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_fetch_weather_timeout_retries():
    """Timeout should trigger retries and return None after all attempts fail."""
    import requests as req

    with patch("producer.fetch.requests.get", side_effect=req.exceptions.Timeout):
        result = fetch_weather(MOCK_CITY)

    assert result is None


def test_fetch_weather_http_error_returns_none():
    """HTTP error should return None immediately without retrying."""
    import requests as req

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = req.exceptions.HTTPError("404")

    with patch("producer.fetch.requests.get", return_value=mock_response):
        result = fetch_weather(MOCK_CITY)

    assert result is None


def test_fetch_weather_unexpected_error_returns_none():
    """Unexpected exception should return None."""
    with patch("producer.fetch.requests.get", side_effect=Exception("Unexpected")):
        result = fetch_weather(MOCK_CITY)

    assert result is None


def test_fetch_all_cities_returns_list():
    """fetch_all_cities should return a list of successful results."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_API_RESPONSE
    mock_response.raise_for_status.return_value = None

    with patch("producer.fetch.requests.get", return_value=mock_response):
        results = fetch_all_cities()

    assert isinstance(results, list)
    assert len(results) == 21


def test_fetch_all_cities_skips_failed():
    """fetch_all_cities should skip cities that fail and return the rest."""
    import requests as req

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            raise req.exceptions.Timeout
        mock = MagicMock()
        mock.json.return_value = MOCK_API_RESPONSE
        mock.raise_for_status.return_value = None
        return mock

    with patch("producer.fetch.requests.get", side_effect=side_effect):
        results = fetch_all_cities()

   assert len(results) == 20
