-- models/staging/stg_local_bike__stores.sql

SELECT
  CAST(store_id AS STRING) AS store_id,
  TRIM(store_name) AS store_name,
  TRIM(phone) AS phone,
  LOWER(TRIM(email)) AS email,
  TRIM(street) AS street,
  TRIM(city) AS city,
  UPPER(TRIM(state)) AS state,
  TRIM(zip_code) AS zip_code
FROM {{ source('local_bike_raw', 'stores') }}
WHERE store_id IS NOT NULL