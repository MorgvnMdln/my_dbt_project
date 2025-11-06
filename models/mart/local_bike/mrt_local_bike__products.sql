-- models/mart/mrt_local_bike__products.sql

WITH product_performance AS (
  SELECT * FROM {{ ref('int_local_bike__product_performance') }}
),

-- Calculate cumulative revenue for ABC analysis
ranked_products AS (
  SELECT
    *,
    SUM(total_net_revenue) OVER () AS total_company_revenue,
    SUM(total_net_revenue) OVER (ORDER BY total_net_revenue DESC) AS cumulative_revenue,
    ROW_NUMBER() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank
  FROM product_performance
),

abc_analysis AS (
  SELECT
    *,
    cumulative_revenue / total_company_revenue AS cumulative_revenue_pct,
    
    -- ABC Classification based on cumulative revenue
    CASE 
      WHEN cumulative_revenue / total_company_revenue <= 0.70 THEN 'A'  -- Top 70% of revenue
      WHEN cumulative_revenue / total_company_revenue <= 0.90 THEN 'B'  -- Next 20% of revenue
      ELSE 'C'  -- Bottom 10% of revenue
    END AS abc_class
    
  FROM ranked_products
)

SELECT
  -- Product identifiers
  product_id,
  product_name,
  brand_name,
  category_name,
  product_classification,
  price_segment,
  model_year,
  model_age_category,
  
  -- Pricing
  avg_list_price,
  
  -- Sales volume metrics
  total_orders,
  total_order_items,
  total_units_sold,
  
  -- Revenue metrics
  total_gross_revenue,
  total_discount_given,
  total_net_revenue,
  
  -- Average metrics
  avg_quantity_per_order,
  avg_discount_percentage,
  avg_net_price_per_unit,
  
  -- Inventory metrics
  total_stock_across_stores,
  total_inventory_value,
  stores_with_stock,
  
  -- Performance indicators
  stock_turnover_ratio,
  avg_daily_units_sold,
  
  -- Temporal metrics
  first_sale_date,
  last_sale_date,
  days_since_last_sale,
  
  -- Product lifecycle
  product_lifecycle_stage,
  
  -- ABC Analysis
  revenue_rank,
  abc_class,
  cumulative_revenue_pct,
  
  -- Revenue contribution
  total_net_revenue / total_company_revenue AS revenue_contribution_pct,
  
  -- Performance flags
  CASE 
    WHEN abc_class = 'A' AND stock_turnover_ratio < 0.5 THEN TRUE
    ELSE FALSE
  END AS needs_restock,
  
  CASE 
    WHEN abc_class IN ('B', 'C') AND total_stock_across_stores > 20 THEN TRUE
    ELSE FALSE
  END AS potential_overstock,
  
  CASE 
    WHEN days_since_last_sale > 180 THEN TRUE
    ELSE FALSE
  END AS is_slow_moving,
  
  CASE 
    WHEN total_units_sold >= 50 AND avg_daily_units_sold >= 0.5 THEN TRUE
    ELSE FALSE
  END AS is_bestseller,
  
  -- Profitability estimate (assuming 40% margin)
  total_net_revenue * 0.40 AS estimated_profit,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM abc_analysis
