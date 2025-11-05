-- models/intermediate/local_bike/int_local_bike__inventory.sql

WITH stocks AS (
  SELECT * FROM {{ ref('stg_local_bike__stocks') }}
),

products AS (
  SELECT * FROM {{ ref('int_local_bike__products') }}
),

stores AS (
  SELECT * FROM {{ ref('stg_local_bike__stores') }}
)

SELECT
  -- Stock identifiers
  stk.stock_id,
  stk.store_id,
  stk.product_id,
  
  -- Store information
  str.store_name,
  str.city AS store_city,
  str.state AS store_state,
  
  -- Product information
  p.product_name,
  p.brand_name,
  p.category_name,
  p.product_classification,
  p.price_segment,
  p.model_year,
  p.model_age_category,
  p.list_price,
  
  -- Inventory metrics
  stk.quantity AS stock_quantity,
  
  -- Stock value
  stk.quantity * p.list_price AS inventory_value,
  
  -- Stock level classification
  CASE 
    WHEN stk.quantity = 0 THEN 'Out of Stock'
    WHEN stk.quantity <= 5 THEN 'Low Stock'
    WHEN stk.quantity <= 15 THEN 'Adequate Stock'
    ELSE 'High Stock'
  END AS stock_level,
  
  -- Stock status flag
  CASE 
    WHEN stk.quantity = 0 THEN TRUE
    ELSE FALSE
  END AS is_out_of_stock,
  
  CASE 
    WHEN stk.quantity > 0 AND stk.quantity <= 5 THEN TRUE
    ELSE FALSE
  END AS is_low_stock

FROM stocks AS stk
LEFT JOIN products AS p ON stk.product_id = p.product_id
LEFT JOIN stores AS str ON stk.store_id = str.store_id
