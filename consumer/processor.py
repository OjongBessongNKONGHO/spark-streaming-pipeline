"""
Batch processor for the Spark Structured Streaming pipeline.
Applies business logic transformations and deduplication
to each micro-batch before it lands in Delta Lake.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    round as spark_round,
    current_timestamp,
    lit,
    expr,
)


def add_temperature_category(df: DataFrame) -> DataFrame:
    """Adds a temperature_category column based on Celsius value."""
    return df.withColumn(
        "temperature_category",
        when(col("temperature") < 0, "freezing")
        .when(col("temperature") < 10, "cold")
        .when(col("temperature") < 20, "mild")
        .when(col("temperature") < 30, "warm")
        .otherwise("hot"),
    )


def add_heat_index(df: DataFrame) -> DataFrame:
    """Adds an approximate heat index combining temperature and humidity."""
    return df.withColumn(
        "heat_index",
        spark_round(
            col("temperature") + (0.33 * col("humidity") / 100 * 6.105) - 4.0, 2
        ),
    )


def add_wind_category(df: DataFrame) -> DataFrame:
    """Adds a wind_category column based on Beaufort scale approximation."""
    return df.withColumn(
        "wind_category",
        when(col("wind_speed") < 1.5, "calm")
        .when(col("wind_speed") < 5.5, "light breeze")
        .when(col("wind_speed") < 10.7, "gentle breeze")
        .when(col("wind_speed") < 17.1, "moderate breeze")
        .when(col("wind_speed") < 24.5, "fresh breeze")
        .otherwise("strong wind"),
    )


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Adds processed_at timestamp and pipeline version for lineage tracking."""
    return df.withColumn("processed_at", current_timestamp()).withColumn(
        "pipeline_version", lit("1.0.0")
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Removes duplicate records within the micro-batch
    based on city and recorded_at combination."""
    return df.dropDuplicates(["city", "recorded_at"])


def transform_batch(df: DataFrame) -> DataFrame:
    """Applies all transformations in sequence to a micro-batch DataFrame.
    Called once per micro-batch by the Spark streaming query."""
    return (
        df.transform(deduplicate)
        .transform(add_temperature_category)
        .transform(add_heat_index)
        .transform(add_wind_category)
        .transform(add_processing_metadata)
    )
