-- models/intermediate/local_bike/int_local_bike__sales_details.sql

WITH orders AS (
  SELECT * FROM {{ ref('int_local_bike__orders') }}
),

order_items AS (
  SELECT * FROM {{ ref('int_local_bike__order_items') }}
)

SELECT
  -- Order information
  o.order_id,
  o.order_date,
  o.required_date,
  o.shipped_date,
  o.order_status,
  o.order_status_label,
  o.days_until_required,
  o.days_to_ship,
  o.shipped_on_time,
  
  -- Time dimensions
  EXTRACT(YEAR FROM o.order_date) AS order_year,
  EXTRACT(MONTH FROM o.order_date) AS order_month,
  EXTRACT(QUARTER FROM o.order_date) AS order_quarter,
  FORMAT_DATE('%Y-%m', o.order_date) AS order_year_month,
  FORMAT_DATE('%A', o.order_date) AS order_day_of_week,
  FORMAT_DATE('%B', o.order_date) AS order_month_name,
  
  -- Customer information
  o.customer_id,
  o.customer_full_name,
  o.customer_email,
  o.customer_city,
  o.customer_state,
  
  -- Store information
  o.store_id,
  o.store_name,
  o.store_city,
  o.store_state,
  
  -- Staff information
  o.staff_id,
  o.staff_full_name,
  o.staff_manager_id,
  
  -- Order item details
  oi.order_item_id,
  oi.item_id,
  oi.product_id,
  oi.product_name,
  oi.brand_name,
  oi.category_name,
  oi.product_classification,
  oi.price_segment,
  oi.model_year,
  oi.model_age_category,
  
  -- Financial metrics
  oi.quantity,
  oi.list_price,
  oi.discount,
  oi.discount_percentage,
  oi.discount_category,
  oi.gross_amount,
  oi.discount_amount,
  oi.total_item_amount AS net_amount,
  oi.net_unit_price

FROM orders AS o
INNER JOIN order_items AS oi ON o.order_id = oi.order_id
