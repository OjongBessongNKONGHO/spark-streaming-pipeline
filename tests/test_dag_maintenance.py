"""
Tests for the Delta Lake maintenance logger.

Tests import directly from jobs/maintenance_logger.py — no Airflow
dependency, no mocking of framework internals. This is the correct
approach: business logic lives in testable modules, DAGs are thin
wrappers. Mirroring the existing pattern where jobs/delta_maintenance.py
and jobs/batch_analysis.py are tested independently of the DAG.

What we're proving:
- log_maintenance_result returns 'completed' when result dict is provided
- log_maintenance_result returns 'no_result' when result is None
- version numbers appear in the log output when maintenance ran
- file counts appear in the log output
- function does not raise on any valid input
- function does not raise when result is None
"""
import logging
import pytest
from jobs.maintenance_logger import log_maintenance_result


@pytest.fixture
def maintenance_result():
    """Realistic maintenance result matching DeltaMaintenanceJob.run() output."""
    return {
        "delta_path": "/tmp/delta/weather",
        "before": {"table_version": 5, "num_files": 20, "size_bytes": 1024},
        "after": {"table_version": 7, "num_files": 8, "size_bytes": 900},
        "optimize": {
            "files_added": 1,
            "files_removed": 12,
            "duration_seconds": 3.2,
        },
        "vacuum_files_eligible": 0,
    }


class TestLogMaintenanceResult:
    def test_returns_completed_when_result_exists(self, maintenance_result):
        """
        A valid maintenance result must return 'completed'.
        The DAG pushes this to XCom for the log_pipeline_run task.
        """
        status = log_maintenance_result(maintenance_result)
        assert status == "completed"

    def test_returns_no_result_when_none(self):
        """
        None result must return 'no_result' without raising.
        The DockerOperator may return None if the container exits
        without printing output — this must not crash the DAG.
        """
        status = log_maintenance_result(None)
        assert status == "no_result"

    def test_does_not_raise_when_result_is_none(self):
        """log_maintenance_result must be non-fatal for missing results."""
        try:
            log_maintenance_result(None)
        except Exception as e:
            pytest.fail(f"log_maintenance_result raised unexpectedly: {e}")

    def test_does_not_raise_for_valid_result(self, maintenance_result):
        """log_maintenance_result must not raise for a valid result."""
        try:
            log_maintenance_result(maintenance_result)
        except Exception as e:
            pytest.fail(f"log_maintenance_result raised unexpectedly: {e}")

    def test_version_numbers_in_log(self, maintenance_result, caplog):
        """
        Before and after table versions must appear in the log output
        so operators can confirm maintenance actually ran.
        """
        with caplog.at_level(logging.INFO):
            log_maintenance_result(maintenance_result)
        combined = " ".join(r.message for r in caplog.records)
        assert "5" in combined
        assert "7" in combined

    def test_file_counts_in_log(self, maintenance_result, caplog):
        """
        Before and after file counts must appear in the log output
        so operators can see compaction effectiveness.
        """
        with caplog.at_level(logging.INFO):
            log_maintenance_result(maintenance_result)
        combined = " ".join(r.message for r in caplog.records)
        assert "20" in combined
        assert "8" in combined

    def test_delta_path_in_log(self, maintenance_result, caplog):
        """The delta path must appear in the log for traceability."""
        with caplog.at_level(logging.INFO):
            log_maintenance_result(maintenance_result)
        combined = " ".join(r.message for r in caplog.records)
        assert "/tmp/delta/weather" in combined

    def test_handles_missing_optimize_key(self):
        """
        Partial result with missing optimize key must not raise.
        DockerOperator output may be incomplete if the job partially failed.
        """
        partial_result = {
            "delta_path": "/tmp/delta/weather",
            "before": {"table_version": 1},
            "after": {"table_version": 2},
        }
        status = log_maintenance_result(partial_result)
        assert status == "completed"

    def test_handles_empty_dict(self):
        """
        Empty dict must return 'completed' without raising.
        The function uses .get() with defaults throughout.
        """
        status = log_maintenance_result({})
        assert status == "completed"