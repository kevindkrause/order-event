{% snapshot item_dim %}

{{
    config(
      target_schema='DW',
      strategy='check',
      unique_key='product_id',
      check_cols="all"
    )

}}

with new_itm as (
  select
      product_id
    ,cast( substr( name, 1, 4000 ) as varchar(4000) ) as item_nm
    ,cast( substr( description, 1, 8000 ) as varchar(8000) ) as item_desc
    ,load_dttm
  from stg.kafka_order_event_product_item_strm
  where ( product_id, load_dttm ) in ( select product_id, max( load_dttm ) from stg.kafka_order_event_product_item_strm group by 1 )
),
existing_itm as (
  select item_key, product_id, item_nm, item_desc, load_dttm
  from dw.item_dim
  where dbt_valid_to is null
)

select coalesce( i.item_key, dw.item_key_seq.nextval ) as item_key, n.product_id, n.item_nm, n.item_desc, n.load_dttm
from new_itm n
left join existing_itm i
  on n.product_id = i.product_id

{% endsnapshot %}