-- models/mart/local_bike/mrt_local_bike__daily_kpis.sql

{{
  config(
    materialized='incremental',
    unique_key='date_day',
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH sales_details AS (
  SELECT * FROM {{ ref('int_local_bike__sales_details') }}
  {% if is_incremental() %}
  WHERE DATE(order_date) > (SELECT MAX(date_day) FROM {{ this }})
  {% endif %}
),

daily_aggregates AS (
  SELECT
    DATE(order_date) AS date_day,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(QUARTER FROM order_date) AS quarter,
    EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week,
    FORMAT_DATE('%A', DATE(order_date)) AS day_name,
    FORMAT_DATE('%Y-%m', DATE(order_date)) AS year_month,
    
    -- Order metrics
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT order_item_id) AS total_items,
    SUM(quantity) AS total_units_sold,
    
    -- Revenue metrics
    SUM(gross_amount) AS total_gross_revenue,
    SUM(discount_amount) AS total_discount_given,
    SUM(net_amount) AS total_net_revenue,
    AVG(net_amount) AS avg_item_revenue,
    
    -- Order value metrics
    SUM(net_amount) / COUNT(DISTINCT order_id) AS avg_order_value,
    SUM(quantity) / COUNT(DISTINCT order_id) AS avg_units_per_order,
    
    -- Discount metrics
    AVG(discount_percentage) AS avg_discount_percentage,
    SUM(discount_amount) / SUM(gross_amount) AS discount_rate,
    
    -- Customer metrics
    COUNT(DISTINCT order_id) / COUNT(DISTINCT customer_id) AS orders_per_customer,
    
    -- Store performance
    COUNT(DISTINCT store_id) AS active_stores,
    COUNT(DISTINCT staff_id) AS active_staff,
    
    -- Product diversity
    COUNT(DISTINCT product_id) AS unique_products_sold,
    COUNT(DISTINCT brand_name) AS unique_brands_sold,
    COUNT(DISTINCT category_name) AS unique_categories_sold,
    
    -- Estimated profit (assuming 40% margin)
    SUM(net_amount) * 0.40 AS estimated_daily_profit
    
  FROM sales_details
  WHERE order_status = 4  -- Completed orders only
  GROUP BY date_day, year, month, quarter, day_of_week, day_name, year_month
)

SELECT
  *,
  
  -- Week-over-week metrics (requires data from previous week)
  LAG(total_net_revenue, 7) OVER (ORDER BY date_day) AS revenue_same_day_last_week,
  total_net_revenue - LAG(total_net_revenue, 7) OVER (ORDER BY date_day) AS revenue_wow_change,
  SAFE_DIVIDE(
    total_net_revenue - LAG(total_net_revenue, 7) OVER (ORDER BY date_day),
    LAG(total_net_revenue, 7) OVER (ORDER BY date_day)
  ) AS revenue_wow_pct_change,
  
  -- 7-day moving averages
  AVG(total_orders) OVER (
    ORDER BY date_day 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS orders_7day_ma,
  
  AVG(total_net_revenue) OVER (
    ORDER BY date_day 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS revenue_7day_ma,
  
  AVG(avg_order_value) OVER (
    ORDER BY date_day 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS aov_7day_ma,
  
  -- 30-day moving averages
  AVG(total_orders) OVER (
    ORDER BY date_day 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS orders_30day_ma,
  
  AVG(total_net_revenue) OVER (
    ORDER BY date_day 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS revenue_30day_ma,
  
  -- Cumulative year-to-date
  SUM(total_net_revenue) OVER (
    PARTITION BY year 
    ORDER BY date_day
  ) AS ytd_revenue,
  
  SUM(total_orders) OVER (
    PARTITION BY year 
    ORDER BY date_day
  ) AS ytd_orders,
  
  -- Cumulative month-to-date
  SUM(total_net_revenue) OVER (
    PARTITION BY year, month 
    ORDER BY date_day
  ) AS mtd_revenue,
  
  SUM(total_orders) OVER (
    PARTITION BY year, month 
    ORDER BY date_day
  ) AS mtd_orders,
  
  -- Performance flags
  CASE 
    WHEN total_net_revenue > AVG(total_net_revenue) OVER (
      ORDER BY date_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) THEN TRUE 
    ELSE FALSE 
  END AS is_above_30day_avg,
  
  CASE 
    WHEN day_of_week IN (1, 7) THEN TRUE  -- Sunday = 1, Saturday = 7
    ELSE FALSE 
  END AS is_weekend,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM daily_aggregates
