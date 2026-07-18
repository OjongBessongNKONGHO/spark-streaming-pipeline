"""
Tests for the Delta Lake maintenance job.

These tests use the same local Spark session pattern as
test_delta_lake_integration.py — Delta Lake extensions loaded via
configure_spark_with_delta_pip, running against a temporary directory.

What we're proving:
- get_table_metrics returns real numbers (file count, size, version)
- OPTIMIZE runs without error and returns a metrics dict with the
  right keys
- VACUUM runs without error
- run() executes the full sequence and returns a report with before/after
  metrics showing the table version incremented
"""

import os
import pytest

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages io.delta:delta-spark_2.12:3.0.0 pyspark-shell",
)

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
from delta import configure_spark_with_delta_pip
from jobs.delta_maintenance import DeltaMaintenanceJob


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

    tmp = tmp_path_factory.mktemp("spark_maintenance")
    builder = (
        SparkSession.builder.appName("TestDeltaMaintenance")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(tmp))
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="module")
def delta_table(spark, tmp_path_factory):
    """
    Creates a Delta table with multiple write batches — simulating
    the small-file accumulation that streaming micro-batches produce.
    """
    schema = StructType(
        [
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("temperature", FloatType(), False),
            StructField("humidity", IntegerType(), False),
            StructField("recorded_at", TimestampType(), False),
        ]
    )

    ts = datetime(2026, 6, 16, 10, 0, 0, tzinfo=timezone.utc)
    data = [
        ("Paris", "FR", 23.5, 65, ts),
        ("Tokyo", "JP", 28.0, 78, ts),
        ("Douala", "CM", 36.0, 85, ts),
    ]

    path = str(tmp_path_factory.mktemp("delta_table"))
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(path)
    df.write.format("delta").mode("append").save(path)
    df.write.format("delta").mode("append").save(path)
    return path


@pytest.fixture(scope="module")
def maintenance(spark):
    """DeltaMaintenanceJob with retention_hours=0 for test environment."""
    return DeltaMaintenanceJob(spark, retention_hours=0)


class TestTableMetrics:
    def test_get_table_metrics_returns_required_keys(self, maintenance, delta_table):
        metrics = maintenance.get_table_metrics(delta_table)
        assert "num_files" in metrics
        assert "size_bytes" in metrics
        assert "table_version" in metrics

    def test_get_table_metrics_values_are_positive(self, maintenance, delta_table):
        metrics = maintenance.get_table_metrics(delta_table)
        assert metrics["num_files"] > 0
        assert metrics["size_bytes"] > 0
        assert metrics["table_version"] >= 0


class TestOptimize:
    def test_optimize_returns_metrics_dict(self, maintenance, delta_table):
        result = maintenance.optimize(delta_table)
        assert "files_added" in result
        assert "files_removed" in result
        assert "duration_seconds" in result

    def test_optimize_duration_is_positive(self, maintenance, delta_table):
        result = maintenance.optimize(delta_table)
        assert result["duration_seconds"] >= 0

    def test_optimize_files_added_is_non_negative(self, maintenance, delta_table):
        """
        OPTIMIZE must return a non-negative files_added count.
        Zero means the table was already compact — valid.
        Version increment is proved in test_run_report_shows_version_increment.
        """
        result = maintenance.optimize(delta_table)
        assert result["files_added"] >= 0
        assert result["files_removed"] >= 0


class TestVacuum:
    def test_vacuum_returns_integer(self, maintenance, delta_table):
        result = maintenance.vacuum(delta_table)
        assert isinstance(result, int)
        assert result >= 0

    def test_vacuum_runs_without_error(self, maintenance, delta_table):
        maintenance.vacuum(delta_table)


class TestMaintenanceRun:
    def test_run_returns_complete_report(self, maintenance, delta_table):
        report = maintenance.run(delta_table)
        assert "delta_path" in report
        assert "before" in report
        assert "after" in report
        assert "optimize" in report
        assert "vacuum_files_eligible" in report

    def test_run_report_shows_version_increment(self, maintenance, delta_table):
        report = maintenance.run(delta_table)
        assert report["after"]["table_version"] > report["before"]["table_version"]

    def test_run_delta_path_in_report(self, maintenance, delta_table):
        report = maintenance.run(delta_table)
        assert report["delta_path"] == delta_table
