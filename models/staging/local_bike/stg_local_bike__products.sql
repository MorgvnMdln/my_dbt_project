-- models/staging/stg_local_bike__products.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('local_bike_raw', 'products') }}
),

cleaned AS (
  SELECT
    -- Primary key
    CAST(product_id AS STRING) AS product_id,
    
    -- Product info
    TRIM(product_name) AS product_name,
    CAST(brand_id AS STRING) AS brand_id,
    CAST(category_id AS STRING) AS category_id,
    
    -- Specs
    CAST(model_year AS INTEGER) AS model_year,
    CAST(list_price AS NUMERIC) AS list_price,
    
    -- Deduplication
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS row_num
    
  FROM source_data
  WHERE product_id IS NOT NULL
)

SELECT
  product_id,
  product_name,
  brand_id,
  category_id,
  model_year,
  list_price
FROM cleaned
WHERE row_num = 1