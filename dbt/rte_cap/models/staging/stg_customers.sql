select
  customer_id,
  name as customer_name,
  email,
  ingested_at
from {{ source('raw', 'raw_customers') }}