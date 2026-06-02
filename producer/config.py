import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "raw_topic": "raw_weather_stream",
    "validated_topic": "validated_weather_stream",
    "invalid_topic": "invalid_weather_stream",
    "schema_registry_url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    "acks": "all",
    "retries": 3,
    "retry_backoff_ms": 500,
    "compression_type": "snappy",
    "linger_ms": 10,
    "batch_size": 16384,
}

CITIES = [
    {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin", "country": "DE", "lat": 52.5200, "lon": 13.4050},
    {"name": "Amsterdam", "country": "NL", "lat": 52.3676, "lon": 4.9041},
    {"name": "Madrid", "country": "ES", "lat": 40.4168, "lon": -3.7038},
    {"name": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"name": "Sao Paulo", "country": "BR", "lat": -23.5505, "lon": -46.6333},
    {"name": "Toronto", "country": "CA", "lat": 43.6532, "lon": -79.3832},
    {"name": "Mexico City", "country": "MX", "lat": 19.4326, "lon": -99.1332},
    {"name": "Buenos Aires", "country": "AR", "lat": -34.6037, "lon": -58.3816},
    {"name": "Douala", "country": "CM", "lat": 4.0511, "lon": 9.7679},
    {"name": "Lagos", "country": "NG", "lat": 6.5244, "lon": 3.3792},
    {"name": "Nairobi", "country": "KE", "lat": -1.2921, "lon": 36.8219},
    {"name": "Cairo", "country": "EG", "lat": 30.0444, "lon": 31.2357},
    {"name": "Johannesburg", "country": "ZA", "lat": -26.2041, "lon": 28.0473},
    {"name": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"name": "Mumbai", "country": "IN", "lat": 19.0760, "lon": 72.8777},
    {"name": "Dubai", "country": "AE", "lat": 25.2048, "lon": 55.2708},
    {"name": "Singapore", "country": "SG", "lat": 1.3521, "lon": 103.8198},
    {"name": "Seoul", "country": "KR", "lat": 37.5665, "lon": 126.9780},
    {"name": "Sydney", "country": "AU", "lat": -33.8688, "lon": 151.2093},
]

API_CONFIG = {
    "base_url": "https://api.openweathermap.org/data/2.5/weather",
    "api_key": os.getenv("OPENWEATHER_API_KEY"),
    "units": "metric",
    "poll_interval_seconds": 30,
    "timeout_seconds": 10,
    "max_retries": 3,
}

LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    "file": "logs/producer.log",
}