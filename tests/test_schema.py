"""
Unit tests for WeatherData Pydantic schema.
Covers valid data, field validators and edge cases.
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError
from producer.schema import WeatherData


VALID_DATA = {
    "city": "paris",
    "country": "FR",
    "temperature": 23.5,
    "feels_like": 22.1,
    "humidity": 65,
    "pressure": 1013,
    "wind_speed": 5.2,
    "wind_direction": 180,
    "visibility": 10000,
    "weather_condition": "Clear",
    "weather_description": "Clear Sky",
    "cloudiness": 10,
    "recorded_at": datetime.now(tz=timezone.utc),
    "latitude": 48.8566,
    "longitude": 2.3522,
}


def test_valid_data_creates_model():
    """Valid data should create a WeatherData instance without errors."""
    weather = WeatherData(**VALID_DATA)
    assert weather.city == "Paris"
    assert weather.temperature == 23.5


def test_city_is_title_cased():
    """City name should be converted to title case."""
    weather = WeatherData(**VALID_DATA)
    assert weather.city == "Paris"


def test_description_is_lowercased():
    """Weather description should be converted to lowercase."""
    weather = WeatherData(**VALID_DATA)
    assert weather.weather_description == "clear sky"


def test_temperature_too_high():
    """Temperature above 60 should raise ValidationError."""
    data = {**VALID_DATA, "temperature": 61.0}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_temperature_too_low():
    """Temperature below -80 should raise ValidationError."""
    data = {**VALID_DATA, "temperature": -81.0}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_humidity_above_100():
    """Humidity above 100 should raise ValidationError."""
    data = {**VALID_DATA, "humidity": 101}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_humidity_below_0():
    """Humidity below 0 should raise ValidationError."""
    data = {**VALID_DATA, "humidity": -1}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_pressure_too_high():
    """Pressure above 1100 should raise ValidationError."""
    data = {**VALID_DATA, "pressure": 1101}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_pressure_too_low():
    """Pressure below 800 should raise ValidationError."""
    data = {**VALID_DATA, "pressure": 799}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_negative_wind_speed():
    """Negative wind speed should raise ValidationError."""
    data = {**VALID_DATA, "wind_speed": -1.0}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_zero_wind_speed_is_valid():
    """Zero wind speed should be valid."""
    data = {**VALID_DATA, "wind_speed": 0.0}
    weather = WeatherData(**data)
    assert weather.wind_speed == 0.0


def test_empty_city_raises_error():
    """Empty city name should raise ValidationError."""
    data = {**VALID_DATA, "city": ""}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_cloudiness_above_100():
    """Cloudiness above 100 should raise ValidationError."""
    data = {**VALID_DATA, "cloudiness": 101}
    with pytest.raises(ValidationError):
        WeatherData(**data)


def test_temperature_is_rounded():
    """Temperature should be rounded to 2 decimal places."""
    data = {**VALID_DATA, "temperature": 23.5678}
    weather = WeatherData(**data)
    assert weather.temperature == 23.57


def test_boundary_temperature_60():
    """Temperature exactly at 60 should be valid."""
    data = {**VALID_DATA, "temperature": 60.0}
    weather = WeatherData(**data)
    assert weather.temperature == 60.0


def test_boundary_temperature_minus_80():
    """Temperature exactly at -80 should be valid."""
    data = {**VALID_DATA, "temperature": -80.0}
    weather = WeatherData(**data)
    assert weather.temperature == -80.0