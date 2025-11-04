-- models/staging/stg_local_bike__brands.sql

SELECT
  CAST(brand_id AS STRING) AS brand_id,
  TRIM(brand_name) AS brand_name
FROM {{ source('local_bike_raw', 'brands') }}
WHERE brand_id IS NOT NULL