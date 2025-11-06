-- models/mart/local_bike/mrt_local_bike__stores.sql

WITH store_performance AS (
  SELECT * FROM {{ ref('int_local_bike__store_performance') }}
),

-- Calculate store rankings
store_rankings AS (
  SELECT
    *,
    -- Revenue ranking
    RANK() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank,
    ROW_NUMBER() OVER (ORDER BY total_net_revenue DESC) AS revenue_row_num,
    
    -- Customer ranking
    RANK() OVER (ORDER BY unique_customers DESC) AS customer_rank,
    
    -- Efficiency ranking (revenue per day)
    RANK() OVER (ORDER BY avg_daily_revenue DESC) AS efficiency_rank,
    
    -- Total stores
    COUNT(*) OVER () AS total_stores
    
  FROM store_performance
)

SELECT
  -- Store identifiers
  store_id,
  store_name,
  store_city,
  store_state,
  
  -- Order metrics
  total_orders,
  unique_customers,
  total_items_sold,
  total_units_sold,
  
  -- Revenue metrics
  total_gross_revenue,
  total_discount_given,
  total_net_revenue,
  
  -- Average metrics
  avg_item_value,
  avg_order_value,
  avg_discount_rate,
  avg_orders_per_customer,
  
  -- Inventory metrics
  total_inventory_units,
  total_inventory_value,
  unique_products_in_stock,
  
  -- Operational metrics
  avg_days_to_ship,
  orders_shipped_on_time,
  orders_with_shipping_info,
  on_time_shipping_rate,
  
  -- Product diversity
  brands_sold,
  categories_sold,
  
  -- Temporal metrics
  first_order_date,
  last_order_date,
  days_in_operation,
  
  -- Daily performance
  avg_daily_revenue,
  avg_daily_orders,
  
  -- Rankings
  revenue_rank,
  customer_rank,
  efficiency_rank,
  
  -- Performance percentile
  SAFE_DIVIDE(revenue_rank, total_stores) AS revenue_percentile,
  
  -- Store performance tier
  CASE 
    WHEN revenue_rank = 1 THEN 'Top Performer'
    WHEN revenue_rank <= CEIL(total_stores * 0.33) THEN 'Above Average'
    WHEN revenue_rank <= CEIL(total_stores * 0.67) THEN 'Average'
    ELSE 'Below Average'
  END AS performance_tier,
  
  -- Health indicators
  CASE 
    WHEN on_time_shipping_rate >= 0.95 THEN 'Excellent'
    WHEN on_time_shipping_rate >= 0.85 THEN 'Good'
    WHEN on_time_shipping_rate >= 0.70 THEN 'Needs Improvement'
    ELSE 'Critical'
  END AS shipping_health,
  
  -- Customer retention indicator
  CASE 
    WHEN avg_orders_per_customer >= 2.0 THEN 'High Retention'
    WHEN avg_orders_per_customer >= 1.5 THEN 'Medium Retention'
    ELSE 'Low Retention'
  END AS retention_indicator,
  
  -- Inventory health
  CASE 
    WHEN total_inventory_value > total_net_revenue THEN 'High Inventory'
    WHEN total_inventory_value > total_net_revenue * 0.5 THEN 'Adequate Inventory'
    ELSE 'Low Inventory'
  END AS inventory_health,
  
  -- Performance flags
  CASE 
    WHEN avg_daily_revenue < (SELECT AVG(avg_daily_revenue) FROM store_performance) THEN TRUE
    ELSE FALSE
  END AS below_avg_revenue,
  
  CASE 
    WHEN on_time_shipping_rate < 0.85 THEN TRUE
    ELSE FALSE
  END AS shipping_issues,
  
  CASE 
    WHEN avg_orders_per_customer < 1.5 THEN TRUE
    ELSE FALSE
  END AS retention_issues,
  
  -- Profitability estimate (assuming 40% margin)
  total_net_revenue * 0.40 AS estimated_profit,
  avg_daily_revenue * 0.40 AS estimated_daily_profit,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM store_rankings
