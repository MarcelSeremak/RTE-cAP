select
  (payload->>'order_id')::bigint            as order_id,
  (payload->>'customer_id')::bigint         as customer_id,
  (payload->>'product_id')::bigint          as product_id,
  coalesce((payload->>'quantity')::int, 1)  as quantity,
  (payload->>'unit_price')::numeric(10,2)   as unit_price,
  (payload->>'currency')                    as currency,
  (payload->>'timestamp')::timestamptz      as order_ts,

  kafka_topic,
  kafka_partition,
  kafka_offset,
  kafka_key,
  ingested_at
from {{ source('raw', 'raw_orders') }}