"""
Kafka topic initialisation script.
Creates raw_weather_stream, validated_weather_stream and
invalid_weather_stream topics with correct partition and
replication settings before the producer starts.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = [
    NewTopic(
        name="raw_weather_stream",
        num_partitions=3,
        replication_factor=1
    ),
    NewTopic(
        name="validated_weather_stream",
        num_partitions=3,
        replication_factor=1
    ),
    NewTopic(
        name="invalid_weather_stream",
        num_partitions=1,
        replication_factor=1
    ),
]


def wait_for_kafka(retries: int = 10, delay: int = 5) -> None:
    """Waits for Kafka to be ready before creating topics."""
    for attempt in range(1, retries + 1):
        try:
            client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            client.close()
            logger.info("Kafka is ready.")
            return
        except Exception:
            logger.warning(f"Kafka not ready. Attempt {attempt}/{retries}. Retrying in {delay}s.")
            time.sleep(delay)
    raise RuntimeError("Kafka did not become ready in time.")


def create_topics() -> None:
    """Creates all three weather topics. Skips existing topics."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            logger.info(f"Created topic: {topic.name} with {topic.num_partitions} partitions")
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists, skipping: {topic.name}")
        except Exception as e:
            logger.error(f"Failed to create topic {topic.name}: {e}")

    admin.close()


if __name__ == "__main__":
    wait_for_kafka()
    create_topics()
    logger.info("All topics initialised.")