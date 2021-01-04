{{ config(
        materialized='incremental',
        unique_key='order_id') }}

with new_order_id as (
    select cast( order_id as number(38) ) as order_id
    from stg.kafka_order_event_id_strm
    group by 1
),
existing_order_id as (
    select order_key, order_id
    from dw.order_key_xref
)

select dw.order_key_seq.nextval as order_key, n.order_id
from new_order_id n
left join existing_order_id e
    on n.order_id = e.order_id
where e.order_key is null