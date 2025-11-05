-- models/intermediate/local_bike/int_local_bike__product_performance.sql

WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
),

inventory AS (
  SELECT * FROM {{ ref('int_local_bike__inventory') }}
)

SELECT
  -- Product information
  sd.product_id,
  sd.product_name,
  sd.brand_name,
  sd.category_name,
  sd.product_classification,
  sd.price_segment,
  sd.model_year,
  sd.model_age_category,
  
  -- Pricing
  AVG(sd.list_price) AS avg_list_price,
  
  -- Sales metrics
  COUNT(DISTINCT sd.order_id) AS total_orders,
  COUNT(DISTINCT sd.order_item_id) AS total_order_items,
  SUM(sd.quantity) AS total_units_sold,
  
  -- Revenue metrics
  SUM(sd.gross_amount) AS total_gross_revenue,
  SUM(sd.discount_amount) AS total_discount_given,
  SUM(sd.net_amount) AS total_net_revenue,
  
  -- Average metrics
  AVG(sd.quantity) AS avg_quantity_per_order,
  AVG(sd.discount_percentage) AS avg_discount_percentage,
  SAFE_DIVIDE(SUM(sd.net_amount), SUM(sd.quantity)) AS avg_net_price_per_unit,
  
  -- Inventory metrics
  SUM(inv.stock_quantity) AS total_stock_across_stores,
  SUM(inv.inventory_value) AS total_inventory_value,
  COUNT(DISTINCT inv.store_id) AS stores_with_stock,
  
  -- Performance indicators
  SAFE_DIVIDE(
    SUM(sd.quantity),
    SUM(inv.stock_quantity)
  ) AS stock_turnover_ratio,
  
  -- Sales velocity (units sold per day since first sale)
  SAFE_DIVIDE(
    SUM(sd.quantity),
    DATE_DIFF(MAX(sd.order_date), MIN(sd.order_date), DAY) + 1
  ) AS avg_daily_units_sold,
  
  -- Date ranges
  MIN(sd.order_date) AS first_sale_date,
  MAX(sd.order_date) AS last_sale_date,
  DATE_DIFF(CURRENT_DATE(), MAX(sd.order_date), DAY) AS days_since_last_sale,
  
  -- Product lifecycle
  CASE 
    WHEN DATE_DIFF(CURRENT_DATE(), MAX(sd.order_date), DAY) > 180 THEN 'Inactive'
    WHEN DATE_DIFF(CURRENT_DATE(), MAX(sd.order_date), DAY) > 90 THEN 'Slow Moving'
    WHEN SUM(sd.quantity) > 50 THEN 'Best Seller'
    WHEN SUM(sd.quantity) > 20 THEN 'Popular'
    ELSE 'Regular'
  END AS product_lifecycle_stage

FROM sales_details AS sd
LEFT JOIN inventory AS inv ON sd.product_id = inv.product_id
WHERE sd.order_status = 4  -- Only completed orders
GROUP BY 
  sd.product_id,
  sd.product_name,
  sd.brand_name,
  sd.category_name,
  sd.product_classification,
  sd.price_segment,
  sd.model_year,
  sd.model_age_category
