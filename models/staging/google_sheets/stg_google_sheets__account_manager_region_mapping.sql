-- models/staging/google_sheets/stg_google_sheets__account_manager_region_mapping.sql

WITH source_data AS (
  SELECT *
  FROM {{ source('google_sheets', 'account_manager_region_mapping') }}
),

cleaned AS (
  SELECT
    TRIM(string_field_0) AS state,
    TRIM(string_field_1) AS account_manager
  FROM source_data
  WHERE string_field_0 IS NOT NULL
    AND string_field_0 != 'State'  -- Exclure la ligne d'en-tête
    AND TRIM(string_field_0) != ''
)

SELECT * FROM cleaned


--select
    --account_manager,
    --state
--from {{ source('google_sheets', 'account_manager_region_mapping') }}
