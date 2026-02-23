select
  product_id,
  name as product_name,
  price,
  category_id,
  ingested_at
from {{ source('raw', 'raw_products') }}