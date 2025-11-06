-- models/staging/stg_local_bike__staffs.sql

SELECT
  CAST(staff_id AS STRING) AS staff_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,
  LOWER(TRIM(email)) AS email,
  TRIM(phone) AS phone,
  CAST(active AS BOOLEAN) AS active,
  CAST(store_id AS STRING) AS store_id,
  CAST(manager_id AS STRING) AS manager_id
FROM {{ source('local_bike_raw', 'staffs') }}
WHERE staff_id IS NOT NULL