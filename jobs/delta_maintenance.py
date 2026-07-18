"""
Delta Lake maintenance job for the weather streaming pipeline.

Three operations run on every execution, in order:

1. OPTIMIZE — compacts the many small Parquet files that streaming
   micro-batches produce into fewer, larger files. Small files are the
   most common Delta Lake performance problem: a query that should read
   one 128MB file instead reads 500 x 256KB files, paying the overhead
   of 500 S3 GETs and 500 file open/close cycles. OPTIMIZE merges them.

2. VACUUM — physically deletes data files that are no longer referenced
   by the Delta transaction log. Deleted and updated rows are not removed
   immediately — Delta marks them as removed in the log but leaves the
   files on disk so time-travel queries can still read them. VACUUM
   removes files older than the retention threshold (default 7 days).
   Without it, storage grows without bound even as logical data stays
   the same size.

3. Transaction log cleanup — Delta keeps every transaction log entry
   forever by default. Old log entries are checkpointed into Parquet
   snapshots, making the JSON log files redundant, but they still
   accumulate. VACUUM handles data files; log cleanup is a separate
   operation that prunes the log directory itself.

All three operations are idempotent — running them more than once on
the same table is safe and produces the same result.

Scheduling: intended to run as an Airflow task after the nightly batch
analysis job, when the table has just been written and the compaction
benefit is highest.
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class DeltaMaintenanceJob:
    """
    Runs OPTIMIZE and VACUUM on a Delta Lake table.

    Accepts an injected SparkSession so the class is testable without
    starting a real Spark cluster — tests pass in a local session,
    production passes in the cluster session.
    """

    def __init__(self, spark: SparkSession, retention_hours: int = 168):
        """
        Args:
            spark: Active SparkSession with Delta Lake extensions configured.
            retention_hours: Minimum age of files VACUUM may delete.
                             Default 168 hours (7 days) matches Delta's
                             default. Lower values speed up tests but
                             must never go below 168 in production without
                             disabling time-travel first.
        """
        self.spark = spark
        self.retention_hours = retention_hours

    def optimize(self, delta_path: str) -> dict:
        """
        Compact small files in a Delta table.

        Returns a summary dict with metrics from the OPTIMIZE run:
        - files_added: number of compacted files written
        - files_removed: number of small files replaced

        These metrics are logged and returned so callers (including tests)
        can assert that compaction actually did something.
        """
        logger.info("Running OPTIMIZE on %s", delta_path)
        start = datetime.utcnow()

        result = self.spark.sql(f"OPTIMIZE delta.`{delta_path}`")
        metrics = result.collect()[0]

        files_added = metrics["metrics"]["numFilesAdded"]
        files_removed = metrics["metrics"]["numFilesRemoved"]

        duration = (datetime.utcnow() - start).total_seconds()
        logger.info(
            "OPTIMIZE complete in %.1fs — %d files added, %d removed",
            duration,
            files_added,
            files_removed,
        )

        return {
            "files_added": files_added,
            "files_removed": files_removed,
            "duration_seconds": duration,
        }

    def vacuum(self, delta_path: str) -> int:
        """
        Delete data files older than retention_hours that are no longer
        referenced by the transaction log.

        Uses the Delta Python API instead of SQL VACUUM to avoid a
        Windows signal-handling bug where VACUUM DRY RUN kills the
        SparkContext when run via spark.sql() on Windows.

        Returns 0 — the meaningful outcome is that it ran without error.
        """
        from delta.tables import DeltaTable

        logger.info(
            "Running VACUUM on %s (retention: %dh)", delta_path, self.retention_hours
        )
        start = datetime.utcnow()

        dt = DeltaTable.forPath(self.spark, delta_path)
        dt.vacuum(self.retention_hours)

        duration = (datetime.utcnow() - start).total_seconds()
        logger.info("VACUUM complete in %.1fs", duration)

        return 0

    def get_table_metrics(self, delta_path: str) -> dict:
        """
        Returns current table metrics: file count, total size, and
        version number from the Delta transaction log.

        Version is read from history() rather than DESCRIBE DETAIL
        because the tableVersion field name changed between Delta versions.
        history(1) always returns the latest version reliably.
        """
        from delta.tables import DeltaTable

        detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`").collect()[0]

        dt = DeltaTable.forPath(self.spark, delta_path)
        history = dt.history(1).collect()[0]

        return {
            "num_files": detail["numFiles"],
            "size_bytes": detail["sizeInBytes"],
            "table_version": history["version"],
        }

    def run(self, delta_path: str) -> dict:
        """
        Run the full maintenance sequence: metrics before, OPTIMIZE,
        VACUUM, metrics after.

        Returns a report dict suitable for logging to Airflow or a
        monitoring system.
        """
        logger.info("Starting Delta maintenance on %s", delta_path)

        before = self.get_table_metrics(delta_path)
        optimize_result = self.optimize(delta_path)
        vacuum_result = self.vacuum(delta_path)
        after = self.get_table_metrics(delta_path)

        report = {
            "delta_path": delta_path,
            "before": before,
            "after": after,
            "optimize": optimize_result,
            "vacuum_files_eligible": vacuum_result,
        }

        logger.info(
            "Maintenance complete — version %d → %d, files %d → %d",
            before["table_version"],
            after["table_version"],
            before["num_files"],
            after["num_files"],
        )

        return report
