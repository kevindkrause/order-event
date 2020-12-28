{{ config(
        materialized='incremental',
        unique_key='row_id') }}

with order_product as (
    select 
         cast( order_id as number(38) ) as order_id
        ,cast( product_id as number(38) ) as product_id
        ,cast( id as number(38) ) as id
        ,cast( vendor_product_id as number(38) ) as vendor_product_id
        ,cast( basket_product_id as number(38) ) as basket_product_id
        ,pos_ref
	    ,name
	    ,description
	    ,special_instructions
	    ,recipient_name
        ,cast( quantity as number(5) ) as quantity
        ,cast( base_cost as number(12,2) ) as base_cost
        ,cast( unit_cost as number(12,2) ) as unit_cost
        ,cast( modifiers as variant ) as modifiers
        ,load_dttm
        ,action
        ,isupdate as is_update
        ,row_id
    from raw.kafka_order_event_product_strm_v
)

select *
from order_product