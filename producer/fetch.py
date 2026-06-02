import requests
from datetime import datetime, timezone
from producer.config import API_CONFIG, CITIES
from producer.logger import get_logger

logger = get_logger(__name__)


def fetch_weather(city: dict) -> dict | None:
    params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "appid": API_CONFIG["api_key"],
        "units": API_CONFIG["units"],
    }

    for attempt in range(1, API_CONFIG["max_retries"] + 1):
        try:
            response = requests.get(
                API_CONFIG["base_url"],
                params=params,
                timeout=API_CONFIG["timeout_seconds"]
            )
            response.raise_for_status()
            data = response.json()

            return {
                "city": city["name"],
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data["wind"]["speed"],
                "wind_direction": data["wind"].get("deg", 0),
                "visibility": data.get("visibility", 0),
                "weather_condition": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "cloudiness": data["clouds"]["all"],
                "recorded_at": datetime.fromtimestamp(
                    data["dt"], tz=timezone.utc
                ).isoformat(),
                "latitude": city["lat"],
                "longitude": city["lon"],
            }

        except requests.exceptions.Timeout:
            logger.warning(
                f"Timeout fetching {city['name']} attempt {attempt}"
            )
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching {city['name']}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {city['name']}: {e}")
            return None

    logger.error(
        f"All {API_CONFIG['max_retries']} attempts failed for {city['name']}"
    )
    return None


def fetch_all_cities() -> list[dict]:
    results = []
    for city in CITIES:
        data = fetch_weather(city)
        if data:
            results.append(data)
            logger.info(
                f"Fetched {city['name']}: {data['temperature']}C "
                f"{data['weather_description']}"
            )
        else:
            logger.warning(f"Skipped {city['name']} — fetch failed")
    return results