-- ─────────────────────────────────────────────────────────────
-- Mart model: mart_city_weather_summary
-- Daily weather summary per city — aggregated from staging.
-- Materialised as a table — pre-computed for fast dashboard queries.
-- ─────────────────────────────────────────────────────────────

WITH staging AS (

    SELECT * FROM {{ ref('stg_weather_events') }}

),

daily_summary AS (

    SELECT
        -- Dimensions
        city,
        country,
        recorded_date,

        -- Temperature aggregates
        ROUND(AVG(temperature_c), 2)        AS avg_temperature_c,
        ROUND(MIN(temperature_c), 2)        AS min_temperature_c,
        ROUND(MAX(temperature_c), 2)        AS max_temperature_c,
        ROUND(MAX(temperature_c)
            - MIN(temperature_c), 2)        AS temperature_range_c,
        ROUND(AVG(feels_like_c), 2)         AS avg_feels_like_c,
        ROUND(AVG(feels_like_gap_c), 2)     AS avg_feels_like_gap_c,

        -- Humidity aggregates
        ROUND(AVG(humidity_pct), 2)         AS avg_humidity_pct,
        ROUND(MIN(humidity_pct), 2)         AS min_humidity_pct,
        ROUND(MAX(humidity_pct), 2)         AS max_humidity_pct,

        -- Pressure aggregates
        ROUND(AVG(pressure_hpa), 2)         AS avg_pressure_hpa,
        ROUND(MIN(pressure_hpa), 2)         AS min_pressure_hpa,
        ROUND(MAX(pressure_hpa), 2)         AS max_pressure_hpa,

        -- Wind aggregates
        ROUND(AVG(wind_speed_ms), 2)        AS avg_wind_speed_ms,
        ROUND(MAX(wind_speed_ms), 2)        AS max_wind_speed_ms,

        -- Most common conditions
        MODE() WITHIN GROUP (
            ORDER BY temperature_category
        )                                   AS dominant_temperature_category,
        MODE() WITHIN GROUP (
            ORDER BY wind_category
        )                                   AS dominant_wind_category,
        MODE() WITHIN GROUP (
            ORDER BY weather_description
        )                                   AS dominant_weather_description,

        -- Record count
        COUNT(*)                            AS record_count

    FROM staging
    GROUP BY city, country, recorded_date

)

SELECT * FROM daily_summary
ORDER BY recorded_date DESC, city