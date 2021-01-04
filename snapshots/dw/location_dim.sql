{% snapshot location_dim %}

{{
    config(
      target_schema='DW',
      strategy='check',
      unique_key='location_id',
      check_cols="all"
    )

}}

with new_loc as (
  select
     location_id
    ,cast( substr( location_name, 1, 4000 ) as varchar(4000) ) as location_nm
    ,brand_id
    ,cast( substr( brand_name, 1, 1000 ) as varchar(1000) ) as brand_nm
    ,cast( substr( brand_guid, 1, 1000 ) as varchar(1000) ) as brand_guid
    ,location_utc_offset as utc_offset_num
    ,cast( location_latitude as number(11,8) ) as latitude_num
    ,cast( location_longitude as number(11,8) ) as longitude_num
    ,load_dttm
  from stg.kafka_order_event_location_strm
  where ( location_id, load_dttm ) in ( select location_id, max( load_dttm ) from stg.kafka_order_event_location_strm group by 1 )
),
existing_loc as (
  select location_key, location_id, location_nm, brand_id, brand_nm, brand_guid, utc_offset_num, latitude_num, longitude_num, load_dttm
  from rpt.location_dim_curr_v
)

select coalesce( l.location_key, dw.location_key_seq.nextval ) as location_key, n.location_id, n.location_nm, n.brand_id, n.brand_nm, n.brand_guid, n.utc_offset_num, n.latitude_num, n.longitude_num, n.load_dttm
from new_loc n
left join existing_loc l
  on n.location_id = l.location_id
  
{% endsnapshot %}