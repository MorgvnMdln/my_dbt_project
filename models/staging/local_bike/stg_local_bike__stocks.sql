-- models/staging/stg_local_bike__stocks.sql

WITH source_data AS (
  SELECT * FROM {{ source('local_bike_raw', 'stocks') }}
)

SELECT
  -- Clé primaire composite
  CONCAT(CAST(store_id AS STRING), '-', CAST(product_id AS STRING)) AS stock_id,
  
  -- Foreign keys
  store_id,
  product_id,
  
  -- Données
  CAST(quantity AS INT64) AS quantity

FROM source_data