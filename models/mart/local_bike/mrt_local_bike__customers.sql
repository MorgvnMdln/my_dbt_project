-- models/mart/local_bike/mrt_local_bike__customers.sql

WITH customer_metrics AS (
  SELECT * FROM {{ ref('int_local_bike__customers') }}
),

-- Calculate RFM scores (1-5, 5 being best)
rfm_scores AS (
  SELECT
    *,
    -- Recency score (lower days = higher score)
    CASE 
      WHEN days_since_last_order <= 30 THEN 5
      WHEN days_since_last_order <= 60 THEN 4
      WHEN days_since_last_order <= 90 THEN 3
      WHEN days_since_last_order <= 180 THEN 2
      ELSE 1
    END AS recency_score,
    
    -- Frequency score
    CASE 
      WHEN total_orders >= 10 THEN 5
      WHEN total_orders >= 5 THEN 4
      WHEN total_orders >= 3 THEN 3
      WHEN total_orders >= 2 THEN 2
      ELSE 1
    END AS frequency_score,
    
    -- Monetary score
    CASE 
      WHEN total_net_revenue >= 5000 THEN 5
      WHEN total_net_revenue >= 2500 THEN 4
      WHEN total_net_revenue >= 1000 THEN 3
      WHEN total_net_revenue >= 500 THEN 2
      ELSE 1
    END AS monetary_score
    
  FROM customer_metrics
)

SELECT
  -- Customer identifiers
  customer_id,
  customer_full_name,
  customer_email,
  customer_city,
  customer_state,
  
  -- Order metrics
  total_orders,
  total_items_purchased,
  total_quantity_purchased,
  
  -- Revenue metrics
  total_gross_revenue,
  total_discount_given,
  total_net_revenue,
  avg_order_value,
  avg_order_item_value,
  avg_discount_rate,
  
  -- Temporal metrics
  first_order_date,
  last_order_date,
  customer_lifetime_days,
  days_since_last_order,
  
  -- RFM Scores
  recency_score,
  frequency_score,
  monetary_score,
  
  -- Combined RFM score (max 15)
  recency_score + frequency_score + monetary_score AS total_rfm_score,
  
  -- RFM segment label
  CASE 
    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'Loyal Customers'
    WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Promising'
    WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
    WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Potential Loyalists'
    WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
    WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Cant Lose Them'
    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Hibernating'
    WHEN recency_score <= 1 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
    ELSE 'Need Attention'
  END AS rfm_segment,
  
  -- Original segment
  customer_segment,
  
  -- Product preferences
  brands_purchased,
  categories_purchased,
  favorite_category,
  
  -- Customer value classification
  CASE
    WHEN total_net_revenue >= 5000 THEN 'High Value'
    WHEN total_net_revenue >= 2000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS customer_value_tier,
  
  -- Churn risk flag
  CASE 
    WHEN days_since_last_order > 180 THEN TRUE
    ELSE FALSE
  END AS is_at_churn_risk,
  
  -- Active customer flag (purchased in last 90 days)
  CASE 
    WHEN days_since_last_order <= 90 THEN TRUE
    ELSE FALSE
  END AS is_active,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM rfm_scores
