-- models/mart/local_bike/mrt_local_bike__inventory.sql

WITH inventory AS (
  SELECT * FROM {{ ref('int_local_bike__inventory') }}
),

product_performance AS (
  SELECT 
    product_id,
    total_units_sold,
    avg_daily_units_sold,
    days_since_last_sale,
    product_lifecycle_stage,
    abc_class
  FROM {{ ref('mrt_local_bike__products') }}
),

inventory_with_sales AS (
  SELECT
    i.*,
    p.total_units_sold,
    p.avg_daily_units_sold,
    p.days_since_last_sale,
    p.product_lifecycle_stage,
    p.abc_class
  FROM inventory AS i
  LEFT JOIN product_performance AS p ON i.product_id = p.product_id
)

SELECT
  -- Identifiers
  stock_id,
  store_id,
  product_id,
  
  -- Store info
  store_name,
  store_city,
  store_state,
  
  -- Product info
  product_name,
  brand_name,
  category_name,
  product_classification,
  price_segment,
  model_year,
  model_age_category,
  list_price,
  
  -- Inventory levels
  stock_quantity,
  inventory_value,
  stock_level,
  is_out_of_stock,
  is_low_stock,
  
  -- Sales performance
  total_units_sold,
  avg_daily_units_sold,
  days_since_last_sale,
  product_lifecycle_stage,
  abc_class,
  
  -- Inventory metrics
  CASE 
    WHEN avg_daily_units_sold > 0 
    THEN stock_quantity / avg_daily_units_sold 
    ELSE NULL 
  END AS days_of_stock_remaining,
  
  CASE 
    WHEN total_units_sold > 0 
    THEN stock_quantity / total_units_sold 
    ELSE NULL 
  END AS stock_to_sales_ratio,
  
  -- Reorder recommendations
  CASE 
    WHEN avg_daily_units_sold > 0 
    THEN GREATEST(0, CEIL((avg_daily_units_sold * 30) - stock_quantity))
    ELSE 0 
  END AS recommended_reorder_quantity,
  
  CASE 
    WHEN avg_daily_units_sold > 0 
    THEN GREATEST(0, ((avg_daily_units_sold * 30) - stock_quantity) * list_price)
    ELSE 0 
  END AS recommended_reorder_value,
  
  -- Alert flags
  CASE 
    WHEN is_out_of_stock = TRUE AND abc_class = 'A' THEN 'CRITICAL'
    WHEN is_out_of_stock = TRUE AND abc_class = 'B' THEN 'HIGH'
    WHEN is_out_of_stock = TRUE THEN 'MEDIUM'
    WHEN is_low_stock = TRUE AND abc_class = 'A' THEN 'HIGH'
    WHEN is_low_stock = TRUE AND abc_class = 'B' THEN 'MEDIUM'
    WHEN is_low_stock = TRUE THEN 'LOW'
    ELSE NULL
  END AS alert_priority,
  
  -- Stock health indicators
  CASE 
    WHEN is_out_of_stock = TRUE THEN 'Out of Stock'
    WHEN avg_daily_units_sold > 0 AND (stock_quantity / avg_daily_units_sold) < 7 THEN 'Critical Low'
    WHEN is_low_stock = TRUE THEN 'Low Stock'
    WHEN avg_daily_units_sold > 0 AND (stock_quantity / avg_daily_units_sold) > 90 THEN 'Overstock'
    WHEN stock_quantity > 50 AND days_since_last_sale > 90 THEN 'Excess/Slow Moving'
    ELSE 'Healthy'
  END AS stock_health,
  
  -- Action recommendations
  CASE 
    WHEN is_out_of_stock = TRUE AND abc_class = 'A' THEN 'URGENT: Reorder immediately - High demand product'
    WHEN is_out_of_stock = TRUE AND abc_class = 'B' THEN 'Reorder soon - Medium demand product'
    WHEN is_out_of_stock = TRUE AND product_lifecycle_stage = 'Inactive' THEN 'Consider discontinuing'
    WHEN is_low_stock = TRUE AND abc_class = 'A' THEN 'Reorder - Running low on bestseller'
    WHEN stock_quantity > 30 AND days_since_last_sale > 180 THEN 'Consider promotion or markdown'
    WHEN stock_quantity > 50 AND avg_daily_units_sold < 0.1 THEN 'Consider transferring to other stores'
    WHEN avg_daily_units_sold > 1 AND (stock_quantity / avg_daily_units_sold) < 14 THEN 'Reorder - Fast moving item'
    ELSE 'No action needed'
  END AS recommended_action,
  
  -- Opportunity flags
  CASE 
    WHEN is_out_of_stock = TRUE AND total_units_sold > 20 THEN TRUE
    ELSE FALSE
  END AS lost_sales_opportunity,
  
  CASE 
    WHEN stock_quantity > 30 AND days_since_last_sale > 90 THEN TRUE
    ELSE FALSE
  END AS markdown_candidate,
  
  CASE 
    WHEN avg_daily_units_sold > 0 AND (stock_quantity / avg_daily_units_sold) < 7 AND abc_class = 'A' THEN TRUE
    ELSE FALSE
  END AS urgent_reorder_needed,
  
  -- Financial impact
  CASE 
    WHEN is_out_of_stock = TRUE AND avg_daily_units_sold > 0 
    THEN avg_daily_units_sold * list_price * 7  -- Estimated lost sales over 1 week
    ELSE 0 
  END AS estimated_weekly_lost_sales,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM inventory_with_sales
