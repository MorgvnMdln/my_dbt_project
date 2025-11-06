-- models/intermediate/local_bike/int_local_bike__staff_performance.sql


WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
)

SELECT
  -- Staff information
  staff_id,
  staff_full_name,
  staff_manager_id,
  
  -- Store context
  store_id,
  store_name,
  store_city,
  store_state,
  
  -- Order metrics
  COUNT(DISTINCT order_id) AS total_orders_processed,
  COUNT(DISTINCT customer_id) AS unique_customers_served,
  COUNT(DISTINCT order_item_id) AS total_items_sold,
  SUM(quantity) AS total_units_sold,
  
  -- Revenue metrics
  SUM(gross_amount) AS total_gross_revenue,
  SUM(discount_amount) AS total_discount_given,
  SUM(net_amount) AS total_net_revenue,
  
  -- Average metrics
  AVG(net_amount) AS avg_item_value,
  SAFE_DIVIDE(SUM(net_amount), COUNT(DISTINCT order_id)) AS avg_order_value,
  SAFE_DIVIDE(SUM(discount_amount), SUM(gross_amount)) AS avg_discount_rate,
  
  -- Customer service metrics
  SAFE_DIVIDE(
    COUNT(DISTINCT order_id),
    COUNT(DISTINCT customer_id)
  ) AS avg_orders_per_customer,
  
  -- Operational metrics
  AVG(days_to_ship) AS avg_days_to_ship,
  COUNTIF(shipped_on_time = TRUE) AS orders_shipped_on_time,
  COUNTIF(shipped_on_time IS NOT NULL) AS orders_with_shipping_info,
  SAFE_DIVIDE(
    COUNTIF(shipped_on_time = TRUE),
    COUNTIF(shipped_on_time IS NOT NULL)
  ) AS on_time_shipping_rate,
  
  -- Order status breakdown
  COUNTIF(order_status = 1) AS pending_orders,
  COUNTIF(order_status = 2) AS processing_orders,
  COUNTIF(order_status = 3) AS rejected_orders,
  COUNTIF(order_status = 4) AS completed_orders,
  
  -- Success rate
  SAFE_DIVIDE(
    COUNTIF(order_status = 4),
    COUNT(DISTINCT order_id)
  ) AS order_completion_rate,
  
  -- Product expertise
  COUNT(DISTINCT brand_name) AS brands_sold,
  COUNT(DISTINCT category_name) AS categories_sold,
  
  -- Date ranges
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date,
  DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1 AS days_active,
  
  -- Daily productivity
  SAFE_DIVIDE(
    COUNT(DISTINCT order_id),
    DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1
  ) AS avg_daily_orders,
  
  SAFE_DIVIDE(
    SUM(net_amount),
    DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1
  ) AS avg_daily_revenue,
  
  -- Performance tier
  CASE 
    WHEN COUNT(DISTINCT order_id) >= 100 THEN 'Top Performer'
    WHEN COUNT(DISTINCT order_id) >= 50 THEN 'High Performer'
    WHEN COUNT(DISTINCT order_id) >= 20 THEN 'Average Performer'
    ELSE 'New/Low Activity'
  END AS performance_tier

FROM sales_details
GROUP BY 
  staff_id,
  staff_full_name,
  staff_manager_id,
  store_id,
  store_name,
  store_city,
  store_state
