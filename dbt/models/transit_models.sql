-- ============================================================
-- dbt models — Transit Delay Platform
-- Layers: staging → intermediate → marts
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- STAGING: stg_trip_delay_events.sql
-- Clean & type-cast raw Spark output from Delta Lake
-- ────────────────────────────────────────────────────────────
-- models/staging/stg_trip_delay_events.sql

{{ config(
    materialized = 'incremental',
    unique_key   = ['trip_id', 'window_start'],
    on_schema_change = 'sync_all_columns',
    incremental_strategy = 'merge',
    file_format  = 'delta',
    partition_by = { 'field': 'window_start', 'data_type': 'timestamp', 'granularity': 'day' }
) }}

WITH source AS (
    SELECT *
    FROM {{ source('delta_lake', 'trip_delay_events') }}
    {% if is_incremental() %}
        WHERE window_start > (SELECT MAX(window_start) FROM {{ this }})
    {% endif %}
)

SELECT
    trip_id,
    route_id,
    vehicle_id,
    agency,

    CAST(window_start AS TIMESTAMP)                 AS window_start,
    CAST(max_arrival_delay_sec AS INT)              AS max_arrival_delay_sec,
    CAST(avg_arrival_delay_sec AS DOUBLE)           AS avg_arrival_delay_sec,
    CAST(stops_reporting AS INT)                    AS stops_reporting,

    UPPER(TRIM(delay_severity))                     AS delay_severity,

    -- Derived flags
    max_arrival_delay_sec >= 300                    AS is_severe,
    max_arrival_delay_sec BETWEEN 60 AND 299        AS is_moderate,
    max_arrival_delay_sec <= 0                      AS is_on_time,

    -- Time dimensions
    DATE(window_start)                              AS event_date,
    HOUR(window_start)                              AS hour_of_day,
    DAYOFWEEK(window_start)                         AS day_of_week,
    HOUR(window_start) BETWEEN 7 AND 9
        OR HOUR(window_start) BETWEEN 16 AND 19     AS is_rush_hour,

    CURRENT_TIMESTAMP()                             AS _loaded_at

FROM source
WHERE
    trip_id    IS NOT NULL
    AND route_id   IS NOT NULL
    AND window_start IS NOT NULL
    -- Sanity bounds
    AND max_arrival_delay_sec BETWEEN -120 AND 7200


-- ────────────────────────────────────────────────────────────
-- STAGING: stg_cascade_flags.sql
-- ────────────────────────────────────────────────────────────
-- models/staging/stg_cascade_flags.sql

{{ config(
    materialized = 'incremental',
    unique_key   = ['route_id', 'window_start'],
    file_format  = 'delta'
) }}

SELECT
    route_id,
    agency,
    CAST(window_start AS TIMESTAMP)     AS window_start,
    CAST(total_trips  AS INT)           AS total_trips,
    CAST(delayed_trips AS INT)          AS delayed_trips,
    ROUND(CAST(delayed_pct AS DOUBLE), 4) AS delayed_pct,
    CAST(is_cascade   AS BOOLEAN)       AS is_cascade,
    CAST(cascade_score AS INT)          AS cascade_score,
    DATE(window_start)                  AS event_date,
    CURRENT_TIMESTAMP()                 AS _loaded_at
FROM {{ source('delta_lake', 'cascade_flags') }}
{% if is_incremental() %}
    WHERE window_start > (SELECT MAX(window_start) FROM {{ this }})
{% endif %}


-- ────────────────────────────────────────────────────────────
-- INTERMEDIATE: int_route_delay_hourly.sql
-- Hourly rollup per route — input to all mart models
-- ────────────────────────────────────────────────────────────
-- models/intermediate/int_route_delay_hourly.sql

{{ config(materialized='table', file_format='delta') }}

WITH trip_hourly AS (
    SELECT
        route_id,
        agency,
        event_date,
        hour_of_day,
        is_rush_hour,
        COUNT(*)                                                AS total_trips,
        SUM(CASE WHEN is_severe   THEN 1 ELSE 0 END)           AS severe_trips,
        SUM(CASE WHEN is_moderate THEN 1 ELSE 0 END)           AS moderate_trips,
        SUM(CASE WHEN is_on_time  THEN 1 ELSE 0 END)           AS on_time_trips,
        AVG(max_arrival_delay_sec)                              AS avg_delay_sec,
        PERCENTILE(max_arrival_delay_sec, 0.5)                 AS p50_delay_sec,
        PERCENTILE(max_arrival_delay_sec, 0.95)                AS p95_delay_sec,
        MAX(max_arrival_delay_sec)                             AS max_delay_sec
    FROM {{ ref('stg_trip_delay_events') }}
    GROUP BY 1,2,3,4,5
),

cascade_hourly AS (
    SELECT
        route_id,
        event_date,
        HOUR(window_start)                                      AS hour_of_day,
        MAX(CAST(is_cascade AS INT))                            AS had_cascade,
        MAX(cascade_score)                                      AS max_cascade_score,
        SUM(CAST(is_cascade AS INT))                            AS cascade_count
    FROM {{ ref('stg_cascade_flags') }}
    GROUP BY 1,2,3
)

SELECT
    t.*,
    COALESCE(c.had_cascade,       0)    AS had_cascade,
    COALESCE(c.max_cascade_score, 0)    AS max_cascade_score,
    COALESCE(c.cascade_count,     0)    AS cascade_count,
    ROUND(severe_trips / NULLIF(total_trips, 0), 4) AS severe_pct,
    ROUND(on_time_trips / NULLIF(total_trips, 0), 4) AS on_time_pct
FROM trip_hourly t
LEFT JOIN cascade_hourly c
    ON  t.route_id   = c.route_id
    AND t.event_date = c.event_date
    AND t.hour_of_day = c.hour_of_day


-- ────────────────────────────────────────────────────────────
-- MART: mart_route_reliability.sql
-- Daily reliability scorecard per route — powers dashboard
-- ────────────────────────────────────────────────────────────
-- models/marts/mart_route_reliability.sql

{{ config(
    materialized = 'table',
    file_format  = 'delta',
    post_hook    = "OPTIMIZE {{ this }} ZORDER BY (route_id, event_date)"
) }}

WITH daily AS (
    SELECT
        route_id,
        agency,
        event_date,
        SUM(total_trips)                    AS daily_trips,
        SUM(severe_trips)                   AS daily_severe,
        SUM(on_time_trips)                  AS daily_on_time,
        AVG(avg_delay_sec)                  AS daily_avg_delay_sec,
        MAX(max_delay_sec)                  AS daily_max_delay_sec,
        PERCENTILE(p95_delay_sec, 0.5)      AS typical_p95_delay_sec,
        MAX(CAST(had_cascade AS INT))       AS had_cascade_event,
        SUM(cascade_count)                  AS total_cascade_windows,
        SUM(CASE WHEN is_rush_hour THEN severe_trips ELSE 0 END) AS rush_hour_severe,
        SUM(CASE WHEN is_rush_hour THEN total_trips  ELSE 0 END) AS rush_hour_trips
    FROM {{ ref('int_route_delay_hourly') }}
    GROUP BY 1,2,3
),

-- 7-day rolling reliability score (0-100, higher = better)
rolling AS (
    SELECT
        *,
        ROUND(
            AVG(ROUND(daily_on_time / NULLIF(daily_trips,0) * 100, 1))
            OVER (PARTITION BY route_id ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        1) AS rolling_7d_on_time_pct,

        ROUND(
            AVG(daily_avg_delay_sec)
            OVER (PARTITION BY route_id ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        1) AS rolling_7d_avg_delay_sec,

        SUM(had_cascade_event)
            OVER (PARTITION BY route_id ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        AS rolling_7d_cascade_count
    FROM daily
)

SELECT
    route_id,
    agency,
    event_date,
    daily_trips,
    daily_on_time,
    daily_severe,
    ROUND(daily_on_time / NULLIF(daily_trips,0) * 100, 1)  AS on_time_pct,
    ROUND(daily_severe  / NULLIF(daily_trips,0) * 100, 1)  AS severe_pct,
    daily_avg_delay_sec,
    daily_max_delay_sec,
    typical_p95_delay_sec,
    had_cascade_event,
    total_cascade_windows,
    rush_hour_severe,
    rush_hour_trips,
    ROUND(rush_hour_severe / NULLIF(rush_hour_trips,0) * 100, 1) AS rush_hour_severe_pct,
    rolling_7d_on_time_pct,
    rolling_7d_avg_delay_sec,
    rolling_7d_cascade_count,
    -- Composite reliability score (0-100)
    GREATEST(0, LEAST(100, ROUND(
        rolling_7d_on_time_pct
        - (rolling_7d_cascade_count * 5)
        - (rolling_7d_avg_delay_sec / 60),
    1))) AS reliability_score
FROM rolling


-- ────────────────────────────────────────────────────────────
-- MART: mart_delay_hotspots.sql
-- Stop-level delay hotspots — powers map layer
-- ────────────────────────────────────────────────────────────
-- models/marts/mart_delay_hotspots.sql

{{ config(materialized='table', file_format='delta') }}

SELECT
    s.stop_id,
    s.stop_name,
    s.latitude,
    s.longitude,
    s.route_id,
    COUNT(*)                                AS delay_events,
    AVG(e.max_arrival_delay_sec)            AS avg_delay_sec,
    MAX(e.max_arrival_delay_sec)            AS max_delay_sec,
    SUM(CAST(e.is_severe AS INT))           AS severe_events,
    ROUND(AVG(e.max_arrival_delay_sec)/60, 1) AS avg_delay_min,
    -- Hotspot score
    ROUND(
        LOG(1 + COUNT(*)) *
        AVG(e.max_arrival_delay_sec) / 60,
    2) AS hotspot_score
FROM {{ ref('stg_trip_delay_events') }} e
JOIN {{ source('static', 'stops') }} s
    ON e.trip_id LIKE '%' || s.stop_id || '%'
WHERE e.event_date >= CURRENT_DATE() - 7
GROUP BY 1,2,3,4,5
HAVING delay_events >= 5
ORDER BY hotspot_score DESC
