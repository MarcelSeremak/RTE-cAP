{{ config(materialized='table') }}

with shipments as (
    select
        shipment_id,
        payment_id,
        order_id,
        customer_id,
        ship_date,
        ingested_at
    from {{ ref('stg_shipments') }}
),

payments as (
    select
        payment_id,
        payment_date,
        amount,
        currency
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
    s.shipment_id,
    s.payment_id,
    s.order_id,
    s.customer_id,
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
    s.ship_date,
    o.order_ts,
    s.ingested_at
from shipments s
left join payments p
    on s.payment_id = p.payment_id
left join orders o
    on s.order_id = o.order_id