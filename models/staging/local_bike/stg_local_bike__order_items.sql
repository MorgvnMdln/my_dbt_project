-- models/staging/local_bike/stg_local_bike__order_items.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('local_bike_raw', 'order_items') }}
),

cleaned AS (
  SELECT
    -- Composite primary key
    CONCAT(CAST(order_id AS STRING), '-', CAST(item_id AS STRING)) AS order_item_id,
    
    -- Colonnes composantes
    CAST(order_id AS STRING) AS order_id,
    CAST(item_id AS INTEGER) AS item_id,
    
    -- Foreign key
    CAST(product_id AS STRING) AS product_id,
    
    -- Metrics
    CAST(quantity AS INTEGER) AS quantity,
    CAST(list_price AS NUMERIC) AS list_price,
    CAST(discount AS NUMERIC) AS discount,
    
    -- Calculated field
    CAST(list_price AS NUMERIC) * CAST(quantity AS INTEGER) * (1 - CAST(discount AS NUMERIC)) AS total_item_amount
    
  FROM source_data
  WHERE order_id IS NOT NULL
    AND item_id IS NOT NULL
    AND quantity > 0
)

SELECT * FROM cleaned