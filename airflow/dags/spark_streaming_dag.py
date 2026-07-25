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
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from jobs.maintenance_logger import log_maintenance_result
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

def log_maintenance_run(**context):
    """
    Thin Airflow wrapper around log_maintenance_result.
    All logic lives in jobs/maintenance_logger.py — testable without Airflow.
    """
    ti = context["ti"]
    maintenance_result = ti.xcom_pull(
        key="maintenance_result", task_ids="run_delta_maintenance"
    )
    status = log_maintenance_result(maintenance_result)
    ti.xcom_push(key="maintenance_status", value=status)

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

    batch_analysis = DockerOperator(
        task_id="run_batch_analysis",
        image="spark-streaming-consumer:latest",
        api_version="auto",
        auto_remove="success",
        command="python3 jobs/batch_analysis.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="spark-streaming-pipeline_default",
        working_dir="/app",
        mount_tmp_dir=False,
        environment={
            "DELTA_LAKE_PATH": os.getenv("DELTA_LAKE_PATH", ""),
            "ANALYTICS_PATH": os.getenv("ANALYTICS_PATH", ""),
            "AWS_REGION": os.getenv("AWS_REGION", "eu-west-3"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
            "S3_BUCKET": os.getenv("S3_BUCKET", ""),
            "AWS_S3_ENDPOINT": os.getenv("AWS_S3_ENDPOINT", ""),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        },
    )

    delta_maintenance = DockerOperator(
        task_id="run_delta_maintenance",
        image="spark-streaming-consumer:latest",
        api_version="auto",
        auto_remove="success",
        command="python3 -c \"\
import json, os, sys; \
sys.path.insert(0, '/app'); \
from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
from jobs.delta_maintenance import DeltaMaintenanceJob; \
builder = SparkSession.builder.appName('DeltaMaintenance') \
    .master('local[2]') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
job = DeltaMaintenanceJob(spark); \
report = job.run(os.getenv('DELTA_LAKE_PATH', '/tmp/delta/weather')); \
print(json.dumps(report)); \
spark.stop() \
\"",
        docker_url="unix://var/run/docker.sock",
        network_mode="spark-streaming-pipeline_default",
        working_dir="/app",
        mount_tmp_dir=False,
        environment={
            "DELTA_LAKE_PATH": os.getenv("DELTA_LAKE_PATH", ""),
            "AWS_REGION": os.getenv("AWS_REGION", "eu-west-3"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
            "S3_BUCKET": os.getenv("S3_BUCKET", ""),
            "AWS_S3_ENDPOINT": os.getenv("AWS_S3_ENDPOINT", ""),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        },
        do_xcom_push=True,
    )

    log_maintenance = PythonOperator(
        task_id="log_maintenance_run",
        python_callable=log_maintenance_run,
    )

    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
    )

    check_kafka >> check_delta >> batch_analysis >> delta_maintenance >> log_maintenance >> log_run
