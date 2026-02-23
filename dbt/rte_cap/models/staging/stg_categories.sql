select
  category_id,
  name as category_name,
  ingested_at
from {{ source('raw', 'raw_categories') }}