select seller_id,
 seller_zip_code,
 seller_city,
 seller_state,
 3 as column_name
from {{ source('sales_database', 'seller') }}