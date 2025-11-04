-- models/staging/local_bike/stg_local_bike__stores.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('local_bike_raw', 'stores') }}
)

SELECT
  -- Primary key
  store_id,
  
  -- Store info
  TRIM(store_name) AS store_name,
  TRIM(phone) AS phone,
  LOWER(TRIM(email)) AS email,
  
  -- Address
  TRIM(street) AS street,
  TRIM(city) AS city,
  TRIM(state) AS state,
  CAST(zip_code AS STRING) AS zip_code

FROM source_data
WHERE store_id IS NOT NULL