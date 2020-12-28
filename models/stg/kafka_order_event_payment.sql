{{ config(
        materialized='incremental',
        unique_key='row_id') }}

with order_payment as (
    select 
         cast( order_id as number(38) ) as order_id
        ,cast( seq_num as number(38) ) as seq_num
        ,payment_type
        ,scheme
        ,description
        ,cast( amount as number(12,2) ) as amount
        ,cast( tip as number(12,2) ) as tip
        ,credit_card
        ,paid_in_advance
        ,load_dttm
        ,action
        ,isupdate as is_update
        ,row_id
    from raw.kafka_order_event_payment_strm_v
)

select *
from order_payment