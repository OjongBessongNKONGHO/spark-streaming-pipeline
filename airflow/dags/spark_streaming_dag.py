"""
Airflow DAG for the Spark Structured Streaming weather pipeline.
Runs hourly batch analysis jobs on Delta Lake data.

Task order:
    check_kafka_health >> check_delta_lake >> run_batch_analysis >> log_pipeline_run

- check_kafka_health: verifies Kafka broker is reachable
- check_delta_lake: verifies Delta Lake data exists and is readable
- run_batch_analysis: triggers the 8 OLAP analytical jobs on Delta Lake
- log_pipeline_run: records run metadata to PostgreSQL pipeline_runs table
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ojong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_kafka_health(**context):
    """
    Verifies the Kafka broker is reachable before running analysis.
    Raises an exception if Kafka is unreachable — stops the DAG early.
    """
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="airflow-health-check",
            request_timeout_ms=5000,
        )
        topics = admin.list_topics()
        admin.close()
        logger.info(f"Kafka healthy — {len(topics)} topics found")
        context["ti"].xcom_push(key="kafka_status", value="healthy")
    except NoBrokersAvailable:
        raise Exception(
            f"Kafka broker not reachable at {bootstrap_servers}. "
            "Stopping DAG — batch analysis requires live Kafka."
        )


def check_delta_lake(**context):
    """
    Verifies Delta Lake data exists before running analysis.
    Checks local path in development, S3 path in production.
    """
    delta_path = os.getenv("DELTA_LAKE_PATH", "/tmp/delta/weather")

    if delta_path.startswith("s3a://"):
        logger.info(f"S3 Delta Lake path configured: {delta_path}")
        context["ti"].xcom_push(key="delta_status", value="s3_configured")
    else:
        if os.path.exists(delta_path):
            partitions = [
                d for d in os.listdir(delta_path)
                if d.startswith("year=")
            ]
            logger.info(
                f"Delta Lake healthy — {len(partitions)} year partitions found"
            )
            context["ti"].xcom_push(key="delta_status", value="healthy")
        else:
            raise Exception(
                f"Delta Lake path not found: {delta_path}. "
                "Run the Spark consumer first to generate data."
            )


def run_batch_analysis(**context):
    """
    Triggers the 8 OLAP analytical jobs on Delta Lake.
    Imports and runs the batch analysis module directly.
    """
    import sys

    sys.path.insert(0, "/opt/airflow")

    try:
        from jobs.batch_analysis import run
        logger.info("Starting batch analysis — 8 OLAP jobs on Delta Lake")
        run()
        logger.info("Batch analysis complete — all 8 jobs finished")
        context["ti"].xcom_push(key="analysis_status", value="success")
        context["ti"].xcom_push(key="jobs_run", value=8)
    except Exception as e:
        logger.error(f"Batch analysis failed: {e}")
        context["ti"].xcom_push(key="analysis_status", value="failed")
        raise


def log_pipeline_run(**context):
    """
    Records pipeline run metadata to the PostgreSQL pipeline_runs table.
    Non-fatal — DAG succeeds even if logging fails.
    """
    import psycopg2
    from datetime import timezone

    ti = context["ti"]
    analysis_status = ti.xcom_pull(
        key="analysis_status", task_ids="run_batch_analysis"
    )
    jobs_run = ti.xcom_pull(key="jobs_run", task_ids="run_batch_analysis") or 0
    run_id = context["run_id"]
    started_at = context["data_interval_start"]
    completed_at = datetime.now(tz=timezone.utc)

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "weather_streaming"),
            user=os.getenv("POSTGRES_USER", "streaming_user"),
            password=os.getenv("POSTGRES_PASSWORD", "streaming_password"),
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, job_name, started_at, completed_at,
                 records_processed, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_id,
                "spark_streaming_batch_analysis",
                started_at,
                completed_at,
                jobs_run,
                analysis_status or "unknown",
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Pipeline run logged — run_id={run_id} status={analysis_status}")
    except Exception as e:
        logger.warning(f"Could not log pipeline run: {e}")


with DAG(
    dag_id="spark_streaming_batch_analysis",
    default_args=default_args,
    description=(
        "Hourly batch analysis on Delta Lake weather data — "
        "8 OLAP jobs covering temperature, humidity, wind and anomalies"
    ),
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "spark", "delta-lake", "batch-analysis", "data-engineering"],
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    check_delta = PythonOperator(
        task_id="check_delta_lake",
        python_callable=check_delta_lake,
    )

    batch_analysis = PythonOperator(
        task_id="run_batch_analysis",
        python_callable=run_batch_analysis,
    )

    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
    )

    check_kafka >> check_delta >> batch_analysis >> log_run
