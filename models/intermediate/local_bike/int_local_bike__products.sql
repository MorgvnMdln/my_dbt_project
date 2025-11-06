-- models/intermediate/local_bike/int_local_bike__products.sql

WITH products AS (
  SELECT * FROM {{ ref('stg_local_bike__products') }}
),

brands AS (
  SELECT * FROM {{ ref('stg_local_bike__brands') }}
),

categories AS (
  SELECT * FROM {{ ref('stg_local_bike__categories') }}
)

SELECT
  -- Product identifiers
  p.product_id,
  p.product_name,
  p.model_year,
  p.list_price,
  
  -- Brand information
  p.brand_id,
  b.brand_name,
  
  -- Category information
  p.category_id,
  c.category_name,
  
  -- Product classification
  CONCAT(b.brand_name, ' - ', c.category_name) AS product_classification,
  
  -- Price segments
  CASE 
    WHEN p.list_price < 500 THEN 'Budget'
    WHEN p.list_price < 1500 THEN 'Mid-Range'
    WHEN p.list_price < 3000 THEN 'Premium'
    ELSE 'Luxury'
  END AS price_segment,
  
  -- Product age (based on model year)
  EXTRACT(YEAR FROM CURRENT_DATE()) - p.model_year AS product_age_years,
  CASE 
    WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - p.model_year = 0 THEN 'Current Year'
    WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - p.model_year = 1 THEN 'Last Year'
    WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - p.model_year >= 2 THEN 'Older Model'
    ELSE 'Future Model'
  END AS model_age_category

FROM products AS p
LEFT JOIN brands AS b ON p.brand_id = b.brand_id
LEFT JOIN categories AS c ON p.category_id = c.category_id
