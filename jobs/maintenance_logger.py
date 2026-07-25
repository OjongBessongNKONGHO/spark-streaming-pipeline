"""
Maintenance run logger for the Delta Lake maintenance Airflow task.

Separated from the DAG so it is testable without Airflow installed.
The DAG calls log_maintenance_result() as a thin wrapper — all logic
lives here, mirroring how batch_analysis.py is separate from the DAG.

Responsibilities:
- Parse the maintenance result dict from the DockerOperator XCom
- Log a human-readable summary of what the maintenance job did
- Return a status string for the pipeline_runs log task
"""
import logging

logger = logging.getLogger(__name__)


def log_maintenance_result(maintenance_result: dict | None) -> str:
    """
    Parse and log the result of a Delta Lake maintenance run.

    Args:
        maintenance_result: dict returned by DeltaMaintenanceJob.run(),
                            or None if the DockerOperator produced no output.

    Returns:
        Status string: "completed" if result exists, "no_result" otherwise.

    The return value is pushed to XCom by the calling task so the
    log_pipeline_run task can include maintenance status in its record.
    """
    if maintenance_result is None:
        logger.warning(
            "No maintenance result received from DockerOperator. "
            "The container may have exited without producing output."
        )
        return "no_result"

    before = maintenance_result.get("before", {})
    after = maintenance_result.get("after", {})
    optimize = maintenance_result.get("optimize", {})
    delta_path = maintenance_result.get("delta_path", "unknown")

    before_version = before.get("table_version", "unknown")
    after_version = after.get("table_version", "unknown")
    before_files = before.get("num_files", "unknown")
    after_files = after.get("num_files", "unknown")
    files_removed = optimize.get("files_removed", 0)
    duration = optimize.get("duration_seconds", 0)

    logger.info(
        "Delta maintenance complete — path=%s version=%s->%s files=%s->%s "
        "compacted=%s duration=%.1fs",
        delta_path,
        before_version,
        after_version,
        before_files,
        after_files,
        files_removed,
        duration,
    )

    return "completed"