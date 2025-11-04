-- models/staging/stg_local_bike__order_items.sql

WITH source_data AS (
  SELECT * FROM {{ source('local_bike_raw', 'order_items') }}
)

SELECT
  -- Clé primaire composite
  CONCAT(CAST(order_id AS STRING), '-', CAST(item_id AS STRING)) AS order_item_id,
  
  -- Foreign keys
  order_id,
  item_id,
  product_id,
  
  -- Données métier
  quantity,
  CAST(list_price AS FLOAT64) AS list_price,
  CAST(discount AS FLOAT64) AS discount,
  
  -- Colonne calculée
  CAST(list_price AS FLOAT64) * quantity * (1 - CAST(discount AS FLOAT64)) AS total_item_amount

FROM source_data