"""
Spark Structured Streaming consumer for the weather pipeline.
Reads from validated_weather_stream Kafka topic in micro-batches,
applies transformations and writes to Delta Lake on S3.
Includes watermarking for late data handling and checkpointing
for fault tolerance and exactly-once semantics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType
)
from consumer.processor import transform_batch
from producer.config import KAFKA_CONFIG
import os


DELTA_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://your-bucket/delta/weather")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://your-bucket/checkpoints/weather")


WEATHER_SCHEMA = StructType([
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
    StructField("schema_version", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Creates and returns a Spark session configured for
    Kafka streaming and Delta Lake on S3."""
    return (
        SparkSession.builder
        .appName("WeatherStreamingPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Reads validated weather messages from Kafka as a structured stream."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"])
        .option("subscribe", KAFKA_CONFIG["validated_topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_stream(raw_stream):
    """Deserialises JSON Kafka messages into a typed Spark DataFrame
    and adds partition columns for efficient Delta Lake storage."""
    return (
        raw_stream
        .select(
            from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
        )
        .select("data.*", "kafka_timestamp", "kafka_offset", "kafka_partition")
        .withColumn("recorded_at", to_timestamp(col("recorded_at")))
        .withWatermark("recorded_at", "10 minutes")
        .withColumn("year", year(col("recorded_at")))
        .withColumn("month", month(col("recorded_at")))
        .withColumn("day", dayofmonth(col("recorded_at")))
        .withColumn("hour", hour(col("recorded_at")))
    )


def write_to_delta(parsed_stream):
    """Writes the parsed stream to Delta Lake partitioned by year, month,
    day and hour. Uses checkpointing for fault tolerance."""
    return (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "hour")
        .start(DELTA_PATH)
    )


def run():
    """Entry point. Creates Spark session, reads from Kafka,
    parses messages and writes to Delta Lake until terminated."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_kafka_stream(spark)
    parsed_stream = parse_stream(raw_stream)
    query = write_to_delta(parsed_stream)

    query.awaitTermination()


if __name__ == "__main__":
    run()