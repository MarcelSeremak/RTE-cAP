{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    concat(kafka_topic, '-', kafka_partition, '-', kafka_offset) as event_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_key,

    (payload ->> 'payment_id')::bigint as payment_id,
    (payload ->> 'order_id')::bigint as order_id,
    (payload ->> 'customer_id')::bigint as customer_id,
    (payload ->> 'amount')::numeric(12,2) as amount,
    payload ->> 'currency' as currency,
    (payload ->> 'payment_date')::timestamptz as payment_date,

    ingested_at
from {{ source('raw', 'raw_events') }}
where kafka_topic = 'payments'

{% if is_incremental() %}
  and ingested_at > (select coalesce(max(ingested_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}