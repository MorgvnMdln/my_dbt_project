-- models/intermediate/local_bike/int_local_bike__store_performance.sql

WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
),

inventory AS (
  SELECT * FROM {{ ref('int_local_bike__inventory') }}
)

SELECT
  -- Store information
  sd.store_id,
  sd.store_name,
  sd.store_city,
  sd.store_state,
  
  -- Order metrics
  COUNT(DISTINCT sd.order_id) AS total_orders,
  COUNT(DISTINCT sd.customer_id) AS unique_customers,
  COUNT(DISTINCT sd.order_item_id) AS total_items_sold,
  SUM(sd.quantity) AS total_units_sold,
  
  -- Revenue metrics
  SUM(sd.gross_amount) AS total_gross_revenue,
  SUM(sd.discount_amount) AS total_discount_given,
  SUM(sd.net_amount) AS total_net_revenue,
  
  -- Average metrics
  AVG(sd.net_amount) AS avg_item_value,
  SAFE_DIVIDE(SUM(sd.net_amount), COUNT(DISTINCT sd.order_id)) AS avg_order_value,
  SAFE_DIVIDE(SUM(sd.discount_amount), SUM(sd.gross_amount)) AS avg_discount_rate,
  
  -- Customer metrics
  SAFE_DIVIDE(
    COUNT(DISTINCT sd.order_id),
    COUNT(DISTINCT sd.customer_id)
  ) AS avg_orders_per_customer,
  
  -- Inventory metrics
  SUM(inv.stock_quantity) AS total_inventory_units,
  SUM(inv.inventory_value) AS total_inventory_value,
  COUNT(DISTINCT inv.product_id) AS unique_products_in_stock,
  
  -- Operational metrics
  AVG(sd.days_to_ship) AS avg_days_to_ship,
  COUNTIF(sd.shipped_on_time = TRUE) AS orders_shipped_on_time,
  COUNTIF(sd.shipped_on_time IS NOT NULL) AS orders_with_shipping_info,
  SAFE_DIVIDE(
    COUNTIF(sd.shipped_on_time = TRUE),
    COUNTIF(sd.shipped_on_time IS NOT NULL)
  ) AS on_time_shipping_rate,
  
  -- Product mix
  COUNT(DISTINCT sd.brand_name) AS brands_sold,
  COUNT(DISTINCT sd.category_name) AS categories_sold,
  
  -- Date ranges
  MIN(sd.order_date) AS first_order_date,
  MAX(sd.order_date) AS last_order_date,
  DATE_DIFF(MAX(sd.order_date), MIN(sd.order_date), DAY) + 1 AS days_in_operation,
  
  -- Daily metrics
  SAFE_DIVIDE(
    SUM(sd.net_amount),
    DATE_DIFF(MAX(sd.order_date), MIN(sd.order_date), DAY) + 1
  ) AS avg_daily_revenue,
  
  SAFE_DIVIDE(
    COUNT(DISTINCT sd.order_id),
    DATE_DIFF(MAX(sd.order_date), MIN(sd.order_date), DAY) + 1
  ) AS avg_daily_orders

FROM sales_details AS sd
LEFT JOIN inventory AS inv ON sd.store_id = inv.store_id
WHERE sd.order_status = 4  -- Only completed orders
GROUP BY 
  sd.store_id,
  sd.store_name,
  sd.store_city,
  sd.store_state
