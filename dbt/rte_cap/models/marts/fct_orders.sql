{{ config(materialized='table') }}

select
  o.order_id,
  o.order_ts,
  o.currency,
  o.customer_id,
  c.customer_name,
  o.product_id,
  p.product_name,
  p.category_id,
  cat.category_name,
  o.quantity,
  o.unit_price,
  (o.quantity * o.unit_price) as revenue,
  o.kafka_topic,
  o.kafka_partition,
  o.kafka_offset,
  o.ingested_at
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customers') }} c
  on c.customer_id = o.customer_id
left join {{ ref('dim_products') }} p
  on p.product_id = o.product_id
left join {{ ref('dim_categories') }} cat
  on cat.category_id = p.category_id