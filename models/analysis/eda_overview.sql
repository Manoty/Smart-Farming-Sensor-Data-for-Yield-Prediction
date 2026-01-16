-- models/analysis/eda_overview.sql
-- Exploratory Data Analysis: Overview of Smart Farming Data

WITH base AS (
    SELECT *
    FROM {{ ref('stg_smart_farming') }}
)

SELECT
    COUNT(DISTINCT farm_id) AS total_farms,
    COUNT(DISTINCT crop_type) AS total_crops,
    COUNT(DISTINCT region) AS total_regions,
    COUNT(*) AS total_records,
    MIN(sowing_date) AS earliest_sowing,
    MAX(harvest_date) AS latest_harvest,
    AVG(yield_kg_per_hectare) AS avg_yield,
    STDDEV(yield_kg_per_hectare) AS yield_stddev,
    AVG(disease_severity_score) AS avg_disease_severity
FROM base;


-- Yield by Crop
SELECT
    crop_type,
    COUNT(*) AS num_records,
    AVG(yield_kg_per_hectare) AS avg_yield,
    STDDEV(yield_kg_per_hectare) AS yield_stddev
FROM {{ ref('stg_smart_farming') }}
GROUP BY crop_type
ORDER BY avg_yield DESC;

-- Yield by Region
SELECT
    region,
    COUNT(*) AS num_records,
    AVG(yield_kg_per_hectare) AS avg_yield
FROM {{ ref('stg_smart_farming') }}
GROUP BY region
ORDER BY avg_yield DESC;

-- Disease Impact
SELECT
    crop_disease_status,
    COUNT(*) AS num_records,
    AVG(yield_kg_per_hectare) AS avg_yield
FROM {{ ref('stg_smart_farming') }}
GROUP BY crop_disease_status
ORDER BY avg_yield ASC;
