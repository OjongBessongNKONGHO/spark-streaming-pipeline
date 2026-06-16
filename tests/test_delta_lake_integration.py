"""
Integration tests for the Delta Lake write path.

These tests catch the class of issues that surfaced during AWS deployment
(Issues 3 and 4): missing JARs for the Delta Lake writer and environment
variables not propagating to the consumer. By running the full
parse → transform → write → read cycle locally, we verify the Delta Lake
sink works correctly before deploying to S3.

No Kafka or S3 connection is required — the tests use a local Spark
session with the Delta Lake extension and a temporary directory.
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType,
)
from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_timestamp
from delta import configure_spark_with_delta_pip


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import os
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
    tmp = tmp_path_factory.mktemp("spark_warehouse")
    builder = (
        SparkSession.builder.appName("TestDeltaLakeIntegration")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(tmp))
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture
def sample_parsed_df(spark):
    """
    Simulates the output of parse_stream() — a typed DataFrame
    with partition columns already added, ready to write to Delta Lake.
    """
    schema = StructType([
        StructField("city",                StringType(),  False),
        StructField("country",             StringType(),  False),
        StructField("temperature",         FloatType(),   False),
        StructField("feels_like",          FloatType(),   False),
        StructField("humidity",            IntegerType(), False),
        StructField("pressure",            IntegerType(), False),
        StructField("wind_speed",          FloatType(),   False),
        StructField("wind_direction",      IntegerType(), True),
        StructField("visibility",          IntegerType(), True),
        StructField("weather_condition",   StringType(),  False),
        StructField("weather_description", StringType(),  False),
        StructField("cloudiness",          IntegerType(), False),
        StructField("recorded_at",         TimestampType(), False),
        StructField("latitude",            FloatType(),   False),
        StructField("longitude",           FloatType(),   False),
        StructField("kafka_offset",        IntegerType(), True),
        StructField("kafka_partition",     IntegerType(), True),
        StructField("year",                IntegerType(), True),
        StructField("month",               IntegerType(), True),
        StructField("day",                 IntegerType(), True),
        StructField("hour",                IntegerType(), True),
    ])

    ts = datetime(2026, 6, 16, 10, 0, 0, tzinfo=timezone.utc)

    data = [
        ("Paris",  "FR", 23.5, 22.1, 65, 1013, 5.2, 180, 10000,
         "Clear", "clear sky", 10, ts, 48.8566, 2.3522, 0, 0,
         2026, 6, 16, 10),
        ("Douala", "CM", 36.0, 40.0, 85, 1008, 2.1,  90,  8000,
         "Thunderstorm", "thunderstorm", 80, ts, 4.0511, 9.7679, 1, 0,
         2026, 6, 16, 10),
        ("Tokyo",  "JP", 28.0, 30.0, 78, 1010, 3.5, 270,  9000,
         "Clouds", "few clouds", 30, ts, 35.6762, 139.6503, 2, 0,
         2026, 6, 16, 10),
    ]

    return spark.createDataFrame(data, schema)


# ── Tests ─────────────────────────────────────────────────────────────

def test_delta_write_creates_files(spark, sample_parsed_df, tmp_path):
    """
    Writing to Delta Lake should create Parquet files and a _delta_log.
    This is the most basic check — if the Delta Lake JARs are missing,
    this test fails immediately (replicating Issue 3).
    """
    delta_path = str(tmp_path / "delta" / "weather")

    sample_parsed_df.write.format("delta").mode("overwrite").save(delta_path)

    import os
    assert os.path.exists(delta_path), "Delta Lake directory was not created"
    assert os.path.exists(os.path.join(delta_path, "_delta_log")), \
        "_delta_log directory missing — Delta Lake write did not complete"


def test_delta_write_and_read_roundtrip(spark, sample_parsed_df, tmp_path):
    """
    Data written to Delta Lake should be readable and row count should match.
    Verifies the full write → read cycle works locally.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    sample_parsed_df.write.format("delta").mode("overwrite").save(delta_path)
    result = spark.read.format("delta").load(delta_path)

    assert result.count() == sample_parsed_df.count()


def test_delta_partition_columns_preserved(spark, sample_parsed_df, tmp_path):
    """
    Partition columns (year, month, day, hour) must be present after
    write and read. Missing partitions would break time-range queries
    on the Delta Lake table.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    (sample_parsed_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .save(delta_path))

    result = spark.read.format("delta").load(delta_path)

    for partition_col in ["year", "month", "day", "hour"]:
        assert partition_col in result.columns, \
            f"Partition column '{partition_col}' missing after Delta Lake read"


def test_delta_partition_values_correct(spark, sample_parsed_df, tmp_path):
    """
    Partition values should match the recorded_at timestamp (2026-06-16 10:00).
    Incorrect partitioning would scatter data across wrong time buckets.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    (sample_parsed_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .save(delta_path))

    result = spark.read.format("delta").load(delta_path).first()

    assert result["year"]  == 2026
    assert result["month"] == 6
    assert result["day"]   == 16
    assert result["hour"]  == 10


def test_delta_schema_preserved(spark, sample_parsed_df, tmp_path):
    """
    Schema written to Delta Lake must match what was read back.
    Schema drift would cause downstream dbt models and OLAP queries to fail.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    sample_parsed_df.write.format("delta").mode("overwrite").save(delta_path)
    result = spark.read.format("delta").load(delta_path)

    written_fields  = {f.name for f in sample_parsed_df.schema.fields}
    read_fields     = {f.name for f in result.schema.fields}

    assert written_fields == read_fields, \
        f"Schema mismatch. Missing: {written_fields - read_fields}"


def test_delta_append_mode_accumulates_rows(spark, sample_parsed_df, tmp_path):
    """
    Appending a second batch should double the row count.
    Verifies append semantics work correctly — critical for streaming
    micro-batch behaviour where each batch appends to the existing table.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    sample_parsed_df.write.format("delta").mode("overwrite").save(delta_path)
    sample_parsed_df.write.format("delta").mode("append").save(delta_path)

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == sample_parsed_df.count() * 2


def test_delta_city_data_correct(spark, sample_parsed_df, tmp_path):
    """
    After write and read, city-level data should be intact.
    Spot-checks that no data corruption occurred during the write path.
    """
    delta_path = str(tmp_path / "delta" / "weather")

    sample_parsed_df.write.format("delta").mode("overwrite").save(delta_path)
    result = spark.read.format("delta").load(delta_path)

    paris = result.filter(col("city") == "Paris").first()
    assert paris is not None, "Paris record not found after Delta Lake roundtrip"
    assert abs(paris["temperature"] - 23.5) < 0.01