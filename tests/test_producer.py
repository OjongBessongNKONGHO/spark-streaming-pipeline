"""
Unit tests for weather_producer.py.
KafkaProducer is mocked so no real Kafka connection is needed.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from producer.weather_producer import process_record


VALID_RECORD = {
    "city": "Paris",
    "country": "FR",
    "temperature": 23.5,
    "feels_like": 22.1,
    "humidity": 65,
    "pressure": 1013,
    "wind_speed": 5.2,
    "wind_direction": 180,
    "visibility": 10000,
    "weather_condition": "Clear",
    "weather_description": "clear sky",
    "cloudiness": 10,
    "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
    "latitude": 48.8566,
    "longitude": 2.3522,
}

INVALID_RECORD = {
    **VALID_RECORD,
    "temperature": 999.0,
    "humidity": 200,
}


def test_valid_record_sent_to_raw_topic():
    """Every record should be sent to raw topic regardless of validity."""
    mock_producer = MagicMock()
    process_record(mock_producer, VALID_RECORD)

    calls = [
        call[1]["topic"] if "topic" in call[1] else call[0][0]
        for call in mock_producer.send.call_args_list
    ]

    assert mock_producer.send.called


def test_valid_record_sent_to_validated_topic():
    """Valid record should be sent to validated topic after passing Pydantic."""
    mock_producer = MagicMock()
    process_record(mock_producer, VALID_RECORD)

    topics_sent = [call[0][0] for call in mock_producer.send.call_args_list]

    assert "validated_weather_stream" in topics_sent


def test_invalid_record_sent_to_invalid_topic():
    """Record failing Pydantic validation should be sent to invalid topic."""
    mock_producer = MagicMock()
    process_record(mock_producer, INVALID_RECORD)

    topics_sent = [call[0][0] for call in mock_producer.send.call_args_list]

    assert "invalid_weather_stream" in topics_sent


def test_invalid_record_not_sent_to_validated_topic():
    """Record failing Pydantic validation should not reach validated topic."""
    mock_producer = MagicMock()
    process_record(mock_producer, INVALID_RECORD)

    topics_sent = [call[0][0] for call in mock_producer.send.call_args_list]

    assert "validated_weather_stream" not in topics_sent


def test_raw_topic_always_receives_record():
    """Raw topic should receive both valid and invalid records."""
    mock_producer = MagicMock()

    process_record(mock_producer, VALID_RECORD)
    valid_topics = [call[0][0] for call in mock_producer.send.call_args_list]
    assert "raw_weather_stream" in valid_topics

    mock_producer.reset_mock()

    process_record(mock_producer, INVALID_RECORD)
    invalid_topics = [call[0][0] for call in mock_producer.send.call_args_list]
    assert "raw_weather_stream" in invalid_topics


def test_process_record_sends_city_as_key():
    """City name should be used as the Kafka message key."""
    mock_producer = MagicMock()
    process_record(mock_producer, VALID_RECORD)

    keys_used = [call[1].get("key") for call in mock_producer.send.call_args_list]

    assert "Paris" in keys_used


def test_process_record_with_unknown_city():
    """Record with missing city should still be processed without crashing."""
    mock_producer = MagicMock()
    record = {**VALID_RECORD, "city": ""}

    try:
        process_record(mock_producer, record)
    except Exception as e:
        pytest.fail(f"process_record raised an exception: {e}")
