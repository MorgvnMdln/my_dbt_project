-- models/intermediate/local_bike/int_local_bike__customers.sql

WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
)

SELECT
  -- Customer information
  customer_id,
  customer_full_name,
  customer_email,
  customer_city,
  customer_state,
  
  -- Order metrics
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT order_item_id) AS total_items_purchased,
  SUM(quantity) AS total_quantity_purchased,
  
  -- Financial metrics
  SUM(gross_amount) AS total_gross_revenue,
  SUM(discount_amount) AS total_discount_given,
  SUM(net_amount) AS total_net_revenue,
  AVG(net_amount) AS avg_order_item_value,
  
  -- Average discount
  SAFE_DIVIDE(SUM(discount_amount), SUM(gross_amount)) AS avg_discount_rate,
  
  -- Order frequency
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date,
  DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS customer_lifetime_days,
  
  -- Customer value
  SAFE_DIVIDE(
    SUM(net_amount),
    COUNT(DISTINCT order_id)
  ) AS avg_order_value,
  
  -- Recency
  DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS days_since_last_order,
  
  -- Customer segmentation
  CASE 
    WHEN COUNT(DISTINCT order_id) = 1 THEN 'One-Time Buyer'
    WHEN COUNT(DISTINCT order_id) <= 3 THEN 'Occasional Buyer'
    WHEN COUNT(DISTINCT order_id) <= 10 THEN 'Regular Buyer'
    ELSE 'VIP Customer'
  END AS customer_segment,
  
  -- Product preferences
  COUNT(DISTINCT brand_name) AS brands_purchased,
  COUNT(DISTINCT category_name) AS categories_purchased,
  
  -- Most purchased category
  ARRAY_AGG(
    category_name ORDER BY total_quantity_purchased DESC LIMIT 1
  )[OFFSET(0)] AS favorite_category

FROM sales_details
WHERE order_status = 4  -- Only completed orders
GROUP BY 
  customer_id,
  customer_full_name,
  customer_email,
  customer_city,
  customer_state
