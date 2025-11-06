-- models/mart/local_bike/mrt_local_bike__sales.sql

{{
  config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["store_id", "customer_id", "product_id"]
  )
}}

WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
)

SELECT
  -- Primary identifiers
  order_id,
  order_item_id,
  
  -- Date dimensions (for time-series analysis)
  order_date,
  order_year,
  order_month,
  order_quarter,
  order_year_month,
  order_day_of_week,
  order_month_name,
  
  -- Order information
  order_status,
  order_status_label,
  required_date,
  shipped_date,
  days_to_ship,
  shipped_on_time,
  
  -- Customer dimension
  customer_id,
  customer_full_name,
  customer_email,
  customer_city,
  customer_state,
  
  -- Product dimension
  product_id,
  product_name,
  brand_name,
  category_name,
  product_classification,
  price_segment,
  model_year,
  model_age_category,
  
  -- Store dimension
  store_id,
  store_name,
  store_city,
  store_state,
  
  -- Staff dimension
  staff_id,
  staff_full_name,
  
  -- Quantity metrics
  quantity,
  
  -- Price metrics
  list_price,
  net_unit_price,
  
  -- Discount metrics
  discount,
  discount_percentage,
  discount_category,
  
  -- Revenue metrics (in dollars)
  gross_amount,
  discount_amount,
  net_amount,
  
  -- Calculated margin (assuming 40% cost)
  net_amount * 0.40 AS estimated_profit,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM sales_details
WHERE order_status = 4  -- Only completed orders in mart
