select
  product_id,
  product_name,
  price,
  category_id
from {{ ref('stg_products') }}