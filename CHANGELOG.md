# Changelog

All notable changes to this project are documented here.

## [0.1.0] - 2026-06-03

### Added
- Kafka producer with Pydantic v2 validation and three-topic routing (raw, validated, invalid)
- Avro schema definition for binary serialisation with Schema Registry
- Spark Structured Streaming consumer writing to Delta Lake with watermarking and checkpointing
- Micro-batch processor with temperature category, heat index, wind category and deduplication
- 8 OLAP batch analysis jobs on Delta Lake data
- Airflow DAG for hourly orchestration with Kafka health check and data quality validation
- dbt project configuration for staging and marts transformation layers
- Terraform modules for networking, compute and storage
- Docker Compose stack with 8 services: Zookeeper, Kafka, Schema Registry, Kafka UI, PostgreSQL, producer, consumer, init-topics
- GitHub Actions CI with Java 17, pyspark and full 39 test suite passing
- 39 pytest unit tests across schema, fetch, producer and consumer modules
- Makefile with shortcuts for up, down, logs, test and clean
- CONTRIBUTING.md with contribution guidelines
- Central config.yaml for all pipeline settings
