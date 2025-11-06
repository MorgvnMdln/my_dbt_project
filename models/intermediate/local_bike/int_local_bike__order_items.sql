-- models/intermediate/local_bike/int_local_bike__order_items.sql

WITH order_items AS (
  SELECT * FROM {{ ref('stg_local_bike__order_items') }}
),

products AS (
  SELECT * FROM {{ ref('int_local_bike__products') }}
)

SELECT
  -- Order item identifiers
  oi.order_item_id,
  oi.order_id,
  oi.item_id,
  
  -- Product information
  oi.product_id,
  p.product_name,
  p.brand_id,
  p.brand_name,
  p.category_id,
  p.category_name,
  p.product_classification,
  p.price_segment,
  p.model_year,
  p.product_age_years,
  p.model_age_category,
  
  -- Pricing and quantities
  oi.quantity,
  oi.list_price,
  oi.discount,
  oi.total_item_amount,
  
  -- Calculated metrics
  oi.list_price * oi.quantity AS gross_amount,
  oi.discount * oi.list_price * oi.quantity AS discount_amount,
  ROUND(oi.discount * 100, 2) AS discount_percentage,
  
  -- Unit economics
  oi.total_item_amount / oi.quantity AS net_unit_price,
  
  -- Discount analysis
  CASE 
    WHEN oi.discount = 0 THEN 'No Discount'
    WHEN oi.discount <= 0.10 THEN 'Low Discount (≤10%)'
    WHEN oi.discount <= 0.25 THEN 'Medium Discount (10-25%)'
    WHEN oi.discount <= 0.50 THEN 'High Discount (25-50%)'
    ELSE 'Very High Discount (>50%)'
  END AS discount_category

FROM order_items AS oi
LEFT JOIN products AS p ON oi.product_id = p.product_id
