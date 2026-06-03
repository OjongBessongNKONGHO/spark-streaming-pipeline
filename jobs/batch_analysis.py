"""
Spark batch analysis job for the weather streaming pipeline.
Reads from Delta Lake, computes 8 OLAP-style analytical aggregations
and writes results back to Delta Lake as separate analytical tables.
Triggered by Airflow on a scheduled basis.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    max,
    min,
    stddev,
    count,
    col,
    round as spark_round,
    rank,
    desc,
)
from pyspark.sql.window import Window
import os

DELTA_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://your-bucket/delta/weather")
ANALYTICS_PATH = os.getenv("ANALYTICS_PATH", "s3a://your-bucket/delta/analytics")


def create_spark_session() -> SparkSession:
    """Creates and returns a Spark session for batch analysis."""
    return (
        SparkSession.builder.appName("WeatherBatchAnalysis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def average_temperature_by_city(df: DataFrame) -> DataFrame:
    """Computes average, max and min temperature per city."""
    return df.groupBy("city", "country").agg(
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        spark_round(max("temperature"), 2).alias("max_temperature"),
        spark_round(min("temperature"), 2).alias("min_temperature"),
        count("*").alias("record_count"),
    )


def city_temperature_rankings(df: DataFrame) -> DataFrame:
    """Ranks cities from hottest to coldest by average temperature."""
    avg_temps = average_temperature_by_city(df)
    window = Window.orderBy(desc("avg_temperature"))
    return avg_temps.withColumn("temperature_rank", rank().over(window))


def humidity_trends(df: DataFrame) -> DataFrame:
    """Computes average humidity per city."""
    return df.groupBy("city", "country").agg(
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
        spark_round(max("humidity"), 2).alias("max_humidity"),
        spark_round(min("humidity"), 2).alias("min_humidity"),
    )


def wind_distribution(df: DataFrame) -> DataFrame:
    """Computes wind speed statistics and dominant wind category per city."""
    return df.groupBy("city", "country", "wind_category").agg(
        spark_round(avg("wind_speed"), 2).alias("avg_wind_speed"),
        spark_round(max("wind_speed"), 2).alias("max_wind_speed"),
        count("*").alias("occurrence_count"),
    )


def condition_frequency(df: DataFrame) -> DataFrame:
    """Counts how often each weather condition occurs per city."""
    return (
        df.groupBy("city", "country", "weather_condition")
        .agg(count("*").alias("occurrence_count"))
        .orderBy("city", desc("occurrence_count"))
    )


def temperature_humidity_correlation(df: DataFrame) -> DataFrame:
    """Computes average heat index per city as a proxy for
    temperature-humidity correlation."""
    return df.groupBy("city", "country").agg(
        spark_round(avg("heat_index"), 2).alias("avg_heat_index"),
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
    )


def daily_temperature_range(df: DataFrame) -> DataFrame:
    """Computes daily temperature range (max minus min) per city."""
    return df.groupBy("city", "country", "year", "month", "day").agg(
        spark_round(max("temperature") - min("temperature"), 2).alias("daily_range"),
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
    )


def anomaly_detection(df: DataFrame) -> DataFrame:
    """Flags records where temperature deviates more than 2 standard
    deviations from the city mean — statistical z-score approach."""
    city_stats = df.groupBy("city").agg(
        avg("temperature").alias("mean_temp"),
        stddev("temperature").alias("stddev_temp"),
    )

    return (
        df.join(city_stats, on="city")
        .withColumn(
            "z_score",
            spark_round(
                (col("temperature") - col("mean_temp")) / col("stddev_temp"), 2
            ),
        )
        .filter(col("z_score").isNotNull())
        .withColumn("is_anomaly", (col("z_score") > 2.0) | (col("z_score") < -2.0))
        .filter(col("is_anomaly"))
        .select("city", "country", "temperature", "recorded_at", "z_score")
    )


def write_analytics(df: DataFrame, table_name: str) -> None:
    """Writes an analytical DataFrame to Delta Lake as a named table."""
    (df.write.format("delta").mode("overwrite").save(f"{ANALYTICS_PATH}/{table_name}"))


def run():
    """Entry point. Reads from Delta Lake, runs all 8 analytical jobs
    and writes results to separate Delta tables."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(DELTA_PATH)
    df.cache()

    write_analytics(average_temperature_by_city(df), "avg_temperature_by_city")
    write_analytics(city_temperature_rankings(df), "city_temperature_rankings")
    write_analytics(humidity_trends(df), "humidity_trends")
    write_analytics(wind_distribution(df), "wind_distribution")
    write_analytics(condition_frequency(df), "condition_frequency")
    write_analytics(temperature_humidity_correlation(df), "temp_humidity_correlation")
    write_analytics(daily_temperature_range(df), "daily_temperature_range")
    write_analytics(anomaly_detection(df), "anomaly_detection")

    spark.stop()


if __name__ == "__main__":
    run()
