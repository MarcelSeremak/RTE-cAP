{{ config(materialized='table') }}

with payments as (
    select
        payment_id,
        order_id,
        customer_id,
        amount,
        currency,
        payment_date,
        ingested_at
    from {{ ref('stg_payments') }}
),

orders as (
    select
        order_id,
        order_ts,
        product_id,
        product_name,
        category_id,
        category_name,
        quantity,
        unit_price,
        revenue,
        currency as order_currency
    from {{ ref('fct_orders') }}
)

select
    p.payment_id,
    p.order_id,
    p.customer_id,
    o.product_id,
    o.product_name,
    o.category_id,
    o.category_name,
    o.quantity,
    o.unit_price,
    o.revenue,
    p.amount as payment_amount,
    p.currency as payment_currency,
    o.order_currency,
    p.payment_date,
    o.order_ts,
    p.ingested_at
from payments p
left join orders o
    on p.order_id = o.order_id