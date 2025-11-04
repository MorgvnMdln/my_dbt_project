-- models/staging/stg_local_bike__orders.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('local_bike_raw', 'orders') }}
),

cleaned AS (
  SELECT
    -- Primary key (INT64)
    order_id,
    
    -- Foreign keys (INT64 pour les jointures)
    customer_id,
    store_id,
    CAST(staff_id AS INT64) AS staff_id,  
    
    -- Status 
    CAST(order_status AS INT64) AS order_status,
    
    -- Dates 
    CAST(order_date AS TIMESTAMP) AS order_date,
    CAST(required_date AS TIMESTAMP) AS required_date, 
    CASE 
      WHEN shipped_date IS NULL THEN NULL
      WHEN UPPER(TRIM(shipped_date)) = 'NULL' THEN NULL
      WHEN TRIM(shipped_date) = '' THEN NULL
      ELSE CAST(shipped_date AS TIMESTAMP)
    END AS shipped_date, 

    -- Deduplication
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS row_num
    
  FROM source_data
  WHERE order_id IS NOT NULL
)

SELECT
  order_id,
  customer_id,
  store_id,
  staff_id,
  order_status,
  order_date,
  required_date,
  shipped_date
FROM cleaned
WHERE row_num = 1