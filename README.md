# Spark Structured Streaming Pipeline

A production-grade real-time data engineering pipeline built with Apache Spark Structured Streaming. A Kafka producer continuously streams live weather data, Spark consumes and processes it in micro-batches, Delta Lake provides ACID storage with time-travel, dbt transforms the data into analytical models, Airflow orchestrates the workflow, and Terraform provisions the AWS infrastructure.

Built as Project 5 of my Data Engineering portfolio, extending Projects 1 to 4 into a unified modern data stack.

## Architecture

OpenWeatherMap API
        |
  [Kafka Producer] --> Kafka Topic (weather_stream)
        |
  [Spark Structured Streaming] --> Delta Lake (S3)
        |
  [dbt Transformation Layer] --> Analytical Models
        |
  [Airflow DAG] --> Orchestration
        |
  [Terraform] --> AWS Infrastructure (EC2, S3, RDS, CloudWatch)

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Stream Ingestion | Apache Kafka | Real-time message queue |
| Stream Processing | Spark Structured Streaming | Micro-batch processing |
| Storage | Delta Lake on S3 | ACID lakehouse storage |
| Transformation | dbt | SQL-based data models |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Infrastructure | Terraform + AWS | Cloud provisioning |
| Containerisation | Docker Compose | Local development |
| CI/CD | GitHub Actions | Automated testing |
| Language | Python 3.11 | Pipeline logic |

## Project Structure

spark-streaming-pipeline/
├── producer/           # Kafka producer — fetches and streams weather data
├── consumer/           # Spark Structured Streaming consumer
├── jobs/               # Spark batch jobs
├── dbt/                # dbt transformation models
├── airflow/
│   ├── dags/           # Airflow DAG definitions
│   └── plugins/        # Custom Airflow operators
├── terraform/
│   └── modules/
│       ├── networking/ # VPC, subnets, security groups
│       ├── compute/    # EC2, IAM
│       └── storage/    # S3, Delta Lake buckets
├── tests/              # pytest unit tests
├── config/             # Configuration files
├── scripts/            # Utility scripts
├── docs/               # Architecture diagrams and documentation
├── docker-compose.yml  # Full stack local setup
├── Makefile            # Shortcuts
├── requirements.txt    # Python dependencies
└── .env.example        # Environment variable template

## Status

In active development — June 2026

## Author

Ojong Bessong NKONGHO
Data Engineering Student — DSTI School of Engineering, Paris
Seeking Data Engineering internship (July 2026) and apprenticeship (September 2026)

LinkedIn: linkedin.com/in/nkongho-ojong
GitHub: github.com/OjongBessongNKONGHO