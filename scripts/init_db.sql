-- Weather streaming pipeline database schema
-- Creates weather_events table, pipeline_runs metadata table
-- and indexes for fast querying

CREATE DATABASE IF NOT EXISTS weather_streaming;

\c weather_streaming;

CREATE USER IF NOT EXISTS streaming_user WITH PASSWORD 'streaming_password';

-- Main weather events table storing transformed Spark output
CREATE TABLE IF NOT EXISTS weather_events (
    id                   SERIAL PRIMARY KEY,
    city                 VARCHAR(100)  NOT NULL,
    country              VARCHAR(10)   NOT NULL,
    temperature          FLOAT         NOT NULL,
    feels_like           FLOAT         NOT NULL,
    humidity             INTEGER       NOT NULL CHECK (humidity >= 0 AND humidity <= 100),
    pressure             INTEGER       NOT NULL CHECK (pressure >= 800 AND pressure <= 1100),
    wind_speed           FLOAT         NOT NULL CHECK (wind_speed >= 0),
    wind_direction       INTEGER,
    visibility           INTEGER,
    weather_condition    VARCHAR(100)  NOT NULL,
    weather_description  VARCHAR(255)  NOT NULL,
    cloudiness           INTEGER       CHECK (cloudiness >= 0 AND cloudiness <= 100),
    temperature_category VARCHAR(20),
    heat_index           FLOAT,
    wind_category        VARCHAR(50),
    latitude             FLOAT,
    longitude            FLOAT,
    recorded_at          TIMESTAMP     NOT NULL,
    processed_at         TIMESTAMP,
    kafka_offset         BIGINT,
    kafka_partition      INTEGER,
    pipeline_version     VARCHAR(20),
    inserted_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline runs metadata table for observability
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_id           VARCHAR(100)  NOT NULL UNIQUE,
    job_name         VARCHAR(100)  NOT NULL,
    started_at       TIMESTAMP     NOT NULL,
    completed_at     TIMESTAMP,
    records_processed INTEGER,
    status           VARCHAR(20)   NOT NULL DEFAULT 'running',
    error_message    TEXT,
    created_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast city and time-range queries
CREATE INDEX IF NOT EXISTS idx_weather_city
    ON weather_events (city);

CREATE INDEX IF NOT EXISTS idx_weather_recorded_at
    ON weather_events (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_city_recorded_at
    ON weather_events (city, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_condition
    ON weather_events (weather_condition);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status);

-- Grant permissions to streaming user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO streaming_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO streaming_user;