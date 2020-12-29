{{ config(
        materialized='incremental',
        unique_key='crc_val' ) }}

with new_item as(
    select
       order_id
      ,product_id
      ,vendor_product_id
      ,basket_product_id
      ,cast( substr( pos_ref, 1, 8000 ) as varchar(8000) ) as pos_ref_cd
      ,cast( substr( special_instructions, 1, 32000 ) as varchar(32000) ) as special_instructions
      ,cast( substr( recipient_name, 1, 8000 ) as varchar(8000) ) as recipient_nm
      ,quantity as item_qty
      ,base_cost as base_cost_amt
      ,unit_cost as unit_cost_amt
      ,load_dttm
    from stg.kafka_order_event_product_strm
    where ( order_id, product_id, load_dttm ) in ( select order_id, product_id, max( load_dttm ) from stg.kafka_order_event_product_strm group by 1, 2 )
),
itm_key as (
    select item_key, product_id
    from rpt.item_dim_curr_v
),
ord_key as (
    select order_key, order_id
    from dw.order_key_xref
)  

select o.order_key, i.item_key, n.order_id, n.product_id, n.vendor_product_id, n.basket_product_id, n.pos_ref_cd, n.special_instructions, n.recipient_nm, n.item_qty, n.base_cost_amt, n.unit_cost_amt, n.load_dttm
    ,cast( n.order_id as varchar(100) ) || '-' || cast( n.product_id as varchar(100) ) as crc_val
from new_item n
inner join itm_key i
    on n.product_id = i.product_id
inner join ord_key o
    on n.order_id = o.order_id    