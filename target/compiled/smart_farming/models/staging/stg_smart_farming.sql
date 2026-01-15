-- =====================================================
-- STAGING MODEL: Smart Farming Sensor Data
-- Purpose:
-- 1. Standardize types
-- 2. Normalize categorical logic
-- 3. Add light exploratory features (clearly labeled)
-- =====================================================

WITH source AS (

    SELECT *
    FROM "smart_farming"."main"."smart_farming_sensor_data"

),

typed AS (

    SELECT
        -- Identifiers
        farm_id,
        sensor_id,

        -- Categorical dimensions
        region,
        crop_type,
        irrigation_type,
        fertilizer_type,
        crop_disease_status,

        -- Environmental metrics
        CAST(soil_moisture_pct AS DOUBLE)     AS soil_moisture_pct,
        CAST(soil_ph AS DOUBLE)               AS soil_ph,
        CAST(temperature_c AS DOUBLE)         AS temperature_c,
        CAST(rainfall_mm AS DOUBLE)            AS rainfall_mm,
        CAST(humidity_pct AS DOUBLE)           AS humidity_pct,
        CAST(sunlight_hours AS DOUBLE)         AS sunlight_hours,
        CAST(ndvi_index AS DOUBLE)             AS ndvi_index,

        -- Operations
        CAST(pesticide_usage_ml AS DOUBLE)     AS pesticide_usage_ml,

        -- Dates and time
        CAST(sowing_date AS DATE)              AS sowing_date,
        CAST(harvest_date AS DATE)             AS harvest_date,
        CAST(total_days AS INTEGER)             AS total_days,
        CAST(timestamp AS TIMESTAMP)           AS sensor_timestamp,

        -- Location
        CAST(latitude AS DOUBLE)               AS latitude,
        CAST(longitude AS DOUBLE)              AS longitude,

        -- Target
        CAST(yield_kg_per_hectare AS DOUBLE)   AS yield_kg_per_hectare

    FROM source
),

enriched AS (

    SELECT
        *,

        -- =================================================
        -- Exploratory feature 1: disease severity score
        -- =================================================
        CASE
            WHEN crop_disease_status = 'None' THEN 0
            WHEN crop_disease_status = 'Mild' THEN 1
            WHEN crop_disease_status = 'Moderate' THEN 2
            WHEN crop_disease_status = 'Severe' THEN 3
            ELSE NULL
        END AS disease_severity_score,

        -- =================================================
        -- Exploratory feature 2: yield per day
        -- Helps normalize across crops with different cycles
        -- =================================================
        CASE
            WHEN total_days > 0
            THEN yield_kg_per_hectare / total_days
            ELSE NULL
        END AS yield_per_day,

        -- =================================================
        -- Exploratory feature 3: NDVI health buckets
        -- Useful for BI and early warning signals
        -- =================================================
        CASE
            WHEN ndvi_index < 0.45 THEN 'Low'
            WHEN ndvi_index BETWEEN 0.45 AND 0.65 THEN 'Medium'
            WHEN ndvi_index > 0.65 THEN 'High'
            ELSE 'Unknown'
        END AS ndvi_health_bucket,

        -- =================================================
        -- Exploratory feature 4: soil pH suitability flag
        -- =================================================
        CASE
            WHEN soil_ph BETWEEN 6.0 AND 7.0 THEN 1
            ELSE 0
        END AS optimal_soil_ph_flag

    FROM typed
)

SELECT *
FROM enriched