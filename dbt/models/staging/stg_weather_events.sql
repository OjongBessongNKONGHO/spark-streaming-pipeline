-- ─────────────────────────────────────────────────────────────
-- Staging model: stg_weather_events
-- Cleans and standardises raw weather events from Delta Lake.
-- Materialised as a view — no storage cost, always fresh.
-- ─────────────────────────────────────────────────────────────

WITH source AS (

    SELECT * FROM {{ source('delta_lake', 'weather_events') }}

),

renamed AS (

    SELECT
        -- Identity
        city                                        AS city,
        country                                     AS country,

        -- Temperature fields — rounded for consistency
        ROUND(temperature, 2)                       AS temperature_c,
        ROUND(feels_like, 2)                        AS feels_like_c,
        ROUND(temperature - feels_like, 2)          AS feels_like_gap_c,

        -- Atmospheric fields
        humidity                                    AS humidity_pct,
        pressure                                    AS pressure_hpa,
        ROUND(wind_speed, 2)                        AS wind_speed_ms,
        wind_direction                              AS wind_direction_deg,
        cloudiness                                  AS cloudiness_pct,
        visibility                                  AS visibility_m,

        -- Weather description
        weather_condition                           AS weather_condition,
        weather_description                         AS weather_description,

        -- Temperature category derived field
        CASE
            WHEN temperature < 0   THEN 'freezing'
            WHEN temperature < 10  THEN 'cold'
            WHEN temperature < 20  THEN 'mild'
            WHEN temperature < 30  THEN 'warm'
            ELSE                        'hot'
        END                                         AS temperature_category,

        -- Wind category derived field
        CASE
            WHEN wind_speed < 1.5  THEN 'calm'
            WHEN wind_speed < 5.5  THEN 'light_breeze'
            WHEN wind_speed < 10.7 THEN 'moderate'
            ELSE                        'strong'
        END                                         AS wind_category,

        -- Location
        latitude                                    AS latitude,
        longitude                                   AS longitude,

        -- Timestamps
        recorded_at                                 AS recorded_at,
        CAST(recorded_at AS DATE)                   AS recorded_date,
        DATE_TRUNC('hour', recorded_at)             AS recorded_hour,

        -- Kafka metadata
        kafka_offset                                AS kafka_offset,
        kafka_partition                             AS kafka_partition

    FROM source

    WHERE
        -- Basic data quality filters
        temperature IS NOT NULL
        AND humidity IS NOT NULL
        AND city IS NOT NULL
        AND recorded_at IS NOT NULL

)

SELECT * FROM renamed