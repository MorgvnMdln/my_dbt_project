-- models/staging/stg_local_bike__categories.sql

SELECT
  CAST(category_id AS STRING) AS category_id,
  TRIM(category_name) AS category_name
FROM {{ source('local_bike_raw', 'categories') }}
WHERE category_id IS NOT NULL