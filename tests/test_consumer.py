"""
Unit tests for consumer/processor.py transformations.
Uses a local Spark session so no cluster is needed.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
)
from datetime import datetime, timezone
from consumer.processor import (
    add_temperature_category,
    add_heat_index,
    add_wind_category,
    deduplicate,
    transform_batch,
)


@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing."""
    return (
        SparkSession.builder.appName("TestWeatherProcessor")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    """Creates a sample DataFrame with two weather records for testing."""
    schema = StructType(
        [
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("temperature", FloatType(), False),
            StructField("feels_like", FloatType(), False),
            StructField("humidity", IntegerType(), False),
            StructField("pressure", IntegerType(), False),
            StructField("wind_speed", FloatType(), False),
            StructField("wind_direction", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("weather_condition", StringType(), False),
            StructField("weather_description", StringType(), False),
            StructField("cloudiness", IntegerType(), False),
            StructField("recorded_at", StringType(), False),
            StructField("latitude", FloatType(), False),
            StructField("longitude", FloatType(), False),
        ]
    )

    data = [
        (
            "Paris",
            "FR",
            23.5,
            22.1,
            65,
            1013,
            5.2,
            180,
            10000,
            "Clear",
            "clear sky",
            10,
            "2026-06-02T12:00:00+00:00",
            48.8566,
            2.3522,
        ),
        (
            "Douala",
            "CM",
            36.0,
            40.0,
            85,
            1008,
            2.1,
            90,
            8000,
            "Thunderstorm",
            "thunderstorm",
            80,
            "2026-06-02T12:00:00+00:00",
            4.0511,
            9.7679,
        ),
        (
            "Paris",
            "FR",
            23.5,
            22.1,
            65,
            1013,
            5.2,
            180,
            10000,
            "Clear",
            "clear sky",
            10,
            "2026-06-02T12:00:00+00:00",
            48.8566,
            2.3522,
        ),
    ]

    return spark.createDataFrame(data, schema)


def test_add_temperature_category_warm(spark, sample_df):
    """Temperature of 23.5 should be categorised as warm."""
    result = add_temperature_category(sample_df)
    paris_row = result.filter(result.city == "Paris").first()
    assert paris_row["temperature_category"] == "warm"


def test_add_temperature_category_hot(spark, sample_df):
    """Temperature of 36.0 should be categorised as hot."""
    result = add_temperature_category(sample_df)
    douala_row = result.filter(result.city == "Douala").first()
    assert douala_row["temperature_category"] == "hot"


def test_add_heat_index_column_exists(spark, sample_df):
    """heat_index column should exist after transformation."""
    result = add_heat_index(sample_df)
    assert "heat_index" in result.columns


def test_add_wind_category_light_breeze(spark, sample_df):
    """Wind speed of 5.2 should be categorised as light breeze."""
    result = add_wind_category(sample_df)
    paris_row = result.filter(result.city == "Paris").first()
    assert paris_row["wind_category"] == "light breeze"


def test_add_wind_category_calm(spark, sample_df):
    """Wind speed of 2.1 should be categorised as calm."""
    result = add_wind_category(sample_df)
    douala_row = result.filter(result.city == "Douala").first()
    assert douala_row["wind_category"] == "calm"


def test_deduplicate_removes_duplicates(spark, sample_df):
    """Deduplication should remove the duplicate Paris record."""
    result = deduplicate(sample_df)
    assert result.count() == 2


def test_transform_batch_adds_all_columns(spark, sample_df):
    """transform_batch should add all derived columns."""
    result = transform_batch(sample_df)
    expected_columns = [
        "temperature_category",
        "heat_index",
        "wind_category",
        "processed_at",
        "pipeline_version",
    ]
    for col in expected_columns:
        assert col in result.columns, f"Missing column: {col}"


def test_transform_batch_deduplicates(spark, sample_df):
    """transform_batch should remove duplicates."""
    result = transform_batch(sample_df)
    assert result.count() == 2


def test_pipeline_version_is_set(spark, sample_df):
    """pipeline_version should be set to 1.0.0."""
    result = transform_batch(sample_df)
    row = result.first()
    assert row["pipeline_version"] == "1.0.0"
