-- Creates a separate database for Airflow's own metadata,
-- kept apart from weather_streaming so orchestration state
-- never shares a schema with application data.
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO streaming_user;