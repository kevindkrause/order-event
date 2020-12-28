{{ config(
    materialized='incremental',
    unique_key="row_id")
 }}

with final as (
    select order_id, seq_num, payment_type, scheme, description, amount, tip, credit_card, paid_in_advance, load_dttm, action, isupdate as is_update, row_id
    from raw.kafka_order_event_payment_strm_v
)

select *
from final