-- models/staging/stg_local_bike__customers.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('local_bike_raw', 'customers') }}
),

cleaned AS (
  SELECT
    -- Primary key (format INT64 dans schema.yml)
    customer_id,
    
    -- Personal info
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    
    -- Contact
    LOWER(TRIM(email)) AS email,
    TRIM(phone) AS phone,
    
    -- Address
    TRIM(street) AS street,
    TRIM(city) AS city,
    TRIM(state) AS state,  -- format texte
    CAST(zip_code AS INT64) AS zip_code,  -- format INT64
    
    -- Deduplication
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS row_num
    
  FROM source_data
  WHERE customer_id IS NOT NULL
)

SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  street,
  city,
  state,
  zip_code
FROM cleaned
WHERE row_num = 1