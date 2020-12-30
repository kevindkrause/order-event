{{ config(
        materialized='incremental',
        unique_key='crc_val' ) }}

with new_pymt as(
    select
       order_id
      ,cast( seq_num as number(5) ) as seq_num
      ,cast( substr( payment_type, 1, 1000 ) as varchar(1000) ) as payment_type
      ,cast( substr( scheme, 1, 8000 ) as varchar(8000) ) as payment_scheme
      ,cast( substr( description, 1, 8000 ) as varchar(8000) ) as payment_desc
      ,amount as payment_amt
      ,tip as tip_amt
      ,cast( substr( credit_card, 1, 8000 ) as varchar(8000) ) as credit_card_desc
      ,cast( case when paid_in_advance then 'Y' else 'N' end as varchar(1) ) as paid_in_advance_flg
      ,load_dttm
    from stg.kafka_order_event_payment_strm
    where ( order_id, seq_num, load_dttm ) in ( select order_id, seq_num, max( load_dttm ) from stg.kafka_order_event_payment_strm group by 1, 2 )
),
ord_key as (
    select order_key, order_id
    --from dw.order_key_xref
    from {{ ref( 'order_key_xref' ) }}
)    

select o.order_key, n.order_id, n.seq_num, n.payment_type, n.payment_scheme, n.payment_desc, n.payment_amt, n.tip_amt, n.credit_card_desc, n.paid_in_advance_flg, n.load_dttm
    ,cast( o.order_key as varchar(100) ) || '-' || cast( n.seq_num as varchar(100) ) as crc_val
from new_pymt n
inner join ord_key o
    on n.order_id = o.order_id