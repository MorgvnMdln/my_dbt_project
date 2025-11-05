-- models/intermediate/local_bike/int_local_bike__orders.sql

WITH orders AS (
  SELECT * FROM {{ ref('stg_local_bike__orders') }}
),

customers AS (
  SELECT * FROM {{ ref('stg_local_bike__customers') }}
),

stores AS (
  SELECT * FROM {{ ref('stg_local_bike__stores') }}
),

staffs AS (
  SELECT * FROM {{ ref('stg_local_bike__staffs') }}
)

SELECT
  -- Order identifiers
  o.order_id,
  o.order_date,
  o.required_date,
  o.shipped_date,
  o.order_status,
  
  -- Order status label
  CASE o.order_status
    WHEN 1 THEN 'Pending'
    WHEN 2 THEN 'Processing'
    WHEN 3 THEN 'Rejected'
    WHEN 4 THEN 'Completed'
    ELSE 'Unknown'
  END AS order_status_label,
  
  -- Delivery metrics
  DATE_DIFF(o.required_date, o.order_date, DAY) AS days_until_required,
  DATE_DIFF(o.shipped_date, o.order_date, DAY) AS days_to_ship,
  CASE 
    WHEN o.shipped_date IS NULL THEN NULL
    WHEN o.shipped_date <= o.required_date THEN TRUE
    ELSE FALSE
  END AS shipped_on_time,
  
  -- Customer information
  o.customer_id,
  c.first_name AS customer_first_name,
  c.last_name AS customer_last_name,
  CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name,
  c.email AS customer_email,
  c.phone AS customer_phone,
  c.city AS customer_city,
  c.state AS customer_state,
  c.zip_code AS customer_zip_code,
  
  -- Store information
  o.store_id,
  st.store_name,
  st.city AS store_city,
  st.state AS store_state,
  st.phone AS store_phone,
  st.email AS store_email,
  
  -- Staff information
  o.staff_id,
  s.full_name AS staff_full_name,
  s.first_name AS staff_first_name,
  s.last_name AS staff_last_name,
  s.email AS staff_email,
  s.active AS staff_active,
  s.manager_id AS staff_manager_id

FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
LEFT JOIN stores AS st ON o.store_id = st.store_id
LEFT JOIN staffs AS s ON o.staff_id = s.staff_id
