from pydantic import BaseModel, field_validator, model_validator
from datetime import datetime
from typing import Optional


class WeatherData(BaseModel):
    city: str
    country: str
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_direction: int
    visibility: int
    weather_condition: str
    weather_description: str
    cloudiness: int
    recorded_at: datetime
    latitude: float
    longitude: float

    @field_validator("city")
    @classmethod
    def city_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("City cannot be empty")
        return v.title()

    @field_validator("temperature", "feels_like")
    @classmethod
    def temperature_range(cls, v):
        if not -80 <= v <= 60:
            raise ValueError(f"Temperature {v} is out of valid range -80 to 60")
        return round(v, 2)

    @field_validator("humidity")
    @classmethod
    def humidity_range(cls, v):
        if not 0 <= v <= 100:
            raise ValueError(f"Humidity {v} must be between 0 and 100")
        return v

    @field_validator("pressure")
    @classmethod
    def pressure_range(cls, v):
        if not 800 <= v <= 1100:
            raise ValueError(f"Pressure {v} must be between 800 and 1100 hPa")
        return v

    @field_validator("wind_speed")
    @classmethod
    def wind_speed_must_be_positive(cls, v):
        if v < 0:
            raise ValueError(f"Wind speed {v} cannot be negative")
        return round(v, 2)

    @field_validator("cloudiness")
    @classmethod
    def cloudiness_range(cls, v):
        if not 0 <= v <= 100:
            raise ValueError(f"Cloudiness {v} must be between 0 and 100")
        return v

    @field_validator("weather_description")
    @classmethod
    def description_lowercase(cls, v):
        return v.lower().strip()

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


class WeatherDataAvro(BaseModel):
    city: str
    country: str
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_direction: int
    visibility: int
    weather_condition: str
    weather_description: str
    cloudiness: int
    recorded_at: str
    latitude: float
    longitude: float
    schema_version: str = "1.0"
