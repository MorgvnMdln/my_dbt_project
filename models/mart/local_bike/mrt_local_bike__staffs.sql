-- models/mart/local_bike/mrt_local_bike__staffs.sql

WITH staff_performance AS (
  SELECT * FROM {{ ref('int_local_bike__staff_performance') }}
),

-- Calculate staff rankings
staff_rankings AS (
  SELECT
    *,
    -- Revenue ranking
    RANK() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank,
    
    -- Productivity ranking
    RANK() OVER (ORDER BY avg_daily_orders DESC) AS productivity_rank,
    
    -- Quality ranking (completion rate + on-time shipping)
    RANK() OVER (
      ORDER BY (order_completion_rate + COALESCE(on_time_shipping_rate, 0)) / 2 DESC
    ) AS quality_rank,
    
    -- Total staff count
    COUNT(*) OVER () AS total_staff,
    
    -- Average metrics for comparison
    AVG(total_net_revenue) OVER () AS avg_staff_revenue,
    AVG(avg_daily_orders) OVER () AS avg_staff_daily_orders
    
  FROM staff_performance
)

SELECT
  -- Staff identifiers
  staff_id,
  staff_full_name,
  staff_manager_id,
  
  -- Store context
  store_id,
  store_name,
  store_city,
  store_state,
  
  -- Order metrics
  total_orders_processed,
  unique_customers_served,
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
  
  -- Operational metrics
  avg_days_to_ship,
  orders_shipped_on_time,
  orders_with_shipping_info,
  on_time_shipping_rate,
  
  -- Order status breakdown
  pending_orders,
  processing_orders,
  rejected_orders,
  completed_orders,
  order_completion_rate,
  
  -- Product expertise
  brands_sold,
  categories_sold,
  
  -- Temporal metrics
  first_order_date,
  last_order_date,
  days_active,
  
  -- Daily productivity
  avg_daily_orders,
  avg_daily_revenue,
  
  -- Performance tier
  performance_tier,
  
  -- Rankings
  revenue_rank,
  productivity_rank,
  quality_rank,
  
  -- Performance vs average
  total_net_revenue / NULLIF(avg_staff_revenue, 0) AS revenue_vs_avg_ratio,
  avg_daily_orders / NULLIF(avg_staff_daily_orders, 0) AS productivity_vs_avg_ratio,
  
  -- Comprehensive performance score (0-100)
  LEAST(100, GREATEST(0,
    (order_completion_rate * 30) +  -- 30% weight on completion
    (COALESCE(on_time_shipping_rate, 0) * 20) +  -- 20% weight on shipping
    (LEAST(1, total_orders_processed / 100.0) * 25) +  -- 25% weight on volume
    (LEAST(1, avg_orders_per_customer / 2.0) * 15) +  -- 15% weight on retention
    (LEAST(1, (1 - avg_discount_rate) * 2) * 10)  -- 10% weight on margin preservation
  )) AS performance_score,
  
  -- Star rating (1-5 stars based on performance score)
  CASE 
    WHEN (order_completion_rate * 30 + COALESCE(on_time_shipping_rate, 0) * 20 + 
          LEAST(1, total_orders_processed / 100.0) * 25 + 
          LEAST(1, avg_orders_per_customer / 2.0) * 15 + 
          LEAST(1, (1 - avg_discount_rate) * 2) * 10) >= 90 THEN 5
    WHEN (order_completion_rate * 30 + COALESCE(on_time_shipping_rate, 0) * 20 + 
          LEAST(1, total_orders_processed / 100.0) * 25 + 
          LEAST(1, avg_orders_per_customer / 2.0) * 15 + 
          LEAST(1, (1 - avg_discount_rate) * 2) * 10) >= 80 THEN 4
    WHEN (order_completion_rate * 30 + COALESCE(on_time_shipping_rate, 0) * 20 + 
          LEAST(1, total_orders_processed / 100.0) * 25 + 
          LEAST(1, avg_orders_per_customer / 2.0) * 15 + 
          LEAST(1, (1 - avg_discount_rate) * 2) * 10) >= 70 THEN 3
    WHEN (order_completion_rate * 30 + COALESCE(on_time_shipping_rate, 0) * 20 + 
          LEAST(1, total_orders_processed / 100.0) * 25 + 
          LEAST(1, avg_orders_per_customer / 2.0) * 15 + 
          LEAST(1, (1 - avg_discount_rate) * 2) * 10) >= 60 THEN 2
    ELSE 1
  END AS star_rating,
  
  -- Strength indicators
  CASE 
    WHEN revenue_rank <= CEIL(total_staff * 0.2) THEN TRUE
    ELSE FALSE
  END AS is_top_revenue_generator,
  
  CASE 
    WHEN productivity_rank <= CEIL(total_staff * 0.2) THEN TRUE
    ELSE FALSE
  END AS is_highly_productive,
  
  CASE 
    WHEN quality_rank <= CEIL(total_staff * 0.2) THEN TRUE
    ELSE FALSE
  END AS is_high_quality,
  
  -- Development areas
  CASE 
    WHEN order_completion_rate < 0.95 THEN TRUE
    ELSE FALSE
  END AS needs_completion_improvement,
  
  CASE 
    WHEN on_time_shipping_rate < 0.90 THEN TRUE
    ELSE FALSE
  END AS needs_shipping_improvement,
  
  CASE 
    WHEN avg_orders_per_customer < 1.5 THEN TRUE
    ELSE FALSE
  END AS needs_retention_training,
  
  CASE 
    WHEN total_orders_processed < 20 THEN TRUE
    ELSE FALSE
  END AS is_new_or_low_activity,
  
  -- Commission/incentive calculation (example: 2% of net revenue)
  total_net_revenue * 0.02 AS incentive_eligible_amount,
  
  -- Profitability contribution (assuming 40% margin)
  total_net_revenue * 0.40 AS profit_contribution,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM staff_rankings
