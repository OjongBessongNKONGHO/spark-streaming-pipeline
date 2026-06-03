"""
Kafka producer for the Spark Structured Streaming pipeline.
Fetches live weather data for 21 cities every 30 seconds and routes
messages across three Kafka topics based on Pydantic v2 validation.
"""

import json
import time
import signal
from kafka import KafkaProducer
from pydantic import ValidationError
from producer.config import KAFKA_CONFIG, API_CONFIG
from producer.schema import WeatherData
from producer.fetch import fetch_all_cities
from producer.logger import get_logger

logger = get_logger(__name__)

running = True


def handle_shutdown(signum, frame):
    """Sets running flag to False on SIGINT or SIGTERM for clean exit."""
    global running
    logger.info("Shutdown signal received.")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def create_producer() -> KafkaProducer:
    """Creates and returns a configured KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=KAFKA_CONFIG["acks"],
        retries=KAFKA_CONFIG["retries"],
        retry_backoff_ms=KAFKA_CONFIG["retry_backoff_ms"],
        compression_type=KAFKA_CONFIG["compression_type"],
        linger_ms=KAFKA_CONFIG["linger_ms"],
        batch_size=KAFKA_CONFIG["batch_size"],
    )


def process_record(producer: KafkaProducer, raw: dict) -> None:
    """Sends record to raw topic then validates and routes to
    validated or invalid topic depending on Pydantic result."""
    city = raw.get("city", "unknown")

    producer.send(KAFKA_CONFIG["raw_topic"], key=city, value=raw)

    try:
        validated = WeatherData(**raw)
        producer.send(
            KAFKA_CONFIG["validated_topic"],
            key=city,
            value=validated.model_dump(mode="json"),
        )
        logger.info(f"Validated and sent: {city} {validated.temperature}C")

    except ValidationError as e:
        producer.send(
            KAFKA_CONFIG["invalid_topic"],
            key=city,
            value={"raw": raw, "errors": e.errors()},
        )
        logger.warning(f"Validation failed for {city}: {e.error_count()} errors")


def run():
    """Main loop. Fetches all cities every 30 seconds until shutdown.
    Always flushes and closes the producer cleanly on exit."""
    logger.info("Starting Spark Structured Streaming Pipeline producer")
    producer = create_producer()

    try:
        while running:
            logger.info("Fetching weather data for all cities")
            records = fetch_all_cities()
            logger.info(f"Fetched {len(records)} records")

            for record in records:
                process_record(producer, record)

            producer.flush()
            logger.info(
                f"Cycle complete. Sleeping {API_CONFIG['poll_interval_seconds']}s"
            )
            time.sleep(API_CONFIG["poll_interval_seconds"])

    finally:
        logger.info("Flushing and closing producer.")
        producer.flush()
        producer.close()
        logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    run()
