{{ config(
        materialized='incremental',
        unique_key='order_key',
        cluster_by=['order_dt'] ) }}

with new_ord as (
    select
       order_id
      ,basket_id
      ,group_order_id
      ,cast( substr( order_guid, 1, 1000 ) as varchar(1000) ) as order_guid 
      ,cast( substr( external_reference, 1, 1000 ) as varchar(1000) ) as external_ref_cd
      ,cast( substr( api_client_reference, 1, 8000 ) as varchar(8000) ) as api_client_ref_cd
      ,cast( substr( pos_ref, 1, 8000 ) as varchar(8000) ) as pos_ref_cd
      ,cast( substr( order_status, 1, 1000 ) as varchar(1000) ) as order_status_cd
      ,date_trunc( 'DAY', time_placed_utc ) as order_dt
      ,time_placed_utc as order_placed_utc_dttm
      ,time_placed_local as order_placed_local_dttm		
      ,time_ready_utc as order_ready_utc_dttm			
      ,time_closed_utc as order_closed_utc_dttm			
      ,time_closed_local as order_closed_local_dttm		
      ,time_cancelled_utc as order_cancelled_utc_dttm		
      ,time_failed_utc as order_failed_utc_dttm		
      ,prep_end_time_utc as order_prep_end_time_utc_dttm		
      ,prep_end_time_local as order_prep_end_time_local_dttm		
      ,time_start_making_utc as order_time_start_making_utc_dttm	
      ,time_start_making_local as order_time_start_making_local_dttm	
      ,time_fired_utc as order_time_fired_utc_dttm			
      ,time_fired_local as order_time_fired_local_dttm		
      ,time_acknowledged_utc as order_time_ack_utc_dttm	
      ,time_acknowledge_local as order_time_ack_local_dttm	
      ,scheduled_fire_time_utc as order_sched_fire_time_utc_dttm	
      ,scheduled_fire_time_local as order_sched_fire_time_local_dttm
      ,time_estimate as time_estimate_min_qty
      ,time_wanted_utc as time_wanted_utc_dttm
      ,time_wanted_local as time_wanted_local_dttm
      ,start_time_utc as start_time_utc_dttm
      ,start_time_local as start_time_local_dttm
      ,utc_offset as utc_offset_num
      ,julian_create_timestamp as julian_create_ts
      ,julian_timestamp_offset as julian_ts_offset
      ,cast( case when was_scheduled = 'true' then 'Y' else 'N' end as varchar(1) ) as sched_flg
      ,cast( case when is_advanced = 'true' then 'Y' else 'N' end as varchar(1) ) as adv_flg
      ,cast( substr( cancel_reason, 1, 8000 ) as varchar(8000) ) as cancel_reason_cd
      ,location_id
      ,cast( substr( external_ref, 1, 8000 ) as varchar(8000) ) as location_external_ref_cd
      ,cast( substr( loyalty_reward_id, 1, 4000 ) as varchar(4000) ) as loyalty_reward_id
      ,cast( substr( transmisison_method, 1, 4000 ) as varchar(4000) ) as transmisison_method_cd  
      ,cast( case when has_price_changed_at_fire_time = 'true' then 'Y' else 'N' end as varchar(1) ) as price_chg_at_fire_time_flg
      ,cast( substr( scheduled_order_price_change_mode, 1, 1000 ) as varchar(1000) ) as sched_order_price_chg_mode_cd  
      ,cast( substr( coupon, 1, 8000 ) as varchar(8000) ) as coupon_cd 
      ,sub_total as sub_total_amt				
      ,sales_tax as sales_tax_amt				
      ,tip as tip_amt	
      ,discount as discount_amt	
      ,net_total as net_total_amt				
      ,total as total_amt					
      ,location_handoff_charge as loc_handoff_charge_amt	
      ,customer_handoff_charge as cust_handoff_charge_amt	
      ,fees as fee_amt 					
      ,vendor_discount as vendor_discount_amt			
      ,vendor_handoff_charge as vendor_handoff_charge_amt 	
      ,vendor_tax as vendor_tax_amt 				
      ,vendor_total as vendor_total_amt			
      ,cast( substr( origin_name, 1, 8000 ) as varchar(8000) ) as origin_nm
	  ,cast( substr( origin_slug, 1, 4000 ) as varchar(4000) ) as origin_slug
	  ,cast(substr( client_platform, 1, 4000 ) as varchar(4000) ) as client_platform_nm
	  ,cast( substr( replaced_order_guid, 1, 1000 ) as varchar(1000) ) as replaced_order_guid
	  ,source_fave as source_fave_num				
	  ,cast( substr( standing_order_template,1, 4000 ) as varchar(4000) ) as standing_order_template_nm		
	  ,cast( substr( origin_source, 1, 4000 ) as varchar(4000) ) as origin_source_nm
	  ,referral_partner_id as referral_partner_id
	  ,cast( substr( third_party_referral, 1, 4000 ) as varchar(4000) ) as third_party_referral_cd		
	  ,cast( substr( handoff_method, 1, 1000 ) as varchar(1000) ) as handoff_method_nm			
	  ,cast( substr( handoff_address, 1, 8000 ) as varchar(8000) ) as handoff_addr			
      ,customer_id as cust_id
	  ,cast( substr( customer_external_reference, 1, 4000 ) as varchar(4000) ) as cust_external_ref_cd
	  ,cast( substr( first_name, 1, 1000 ) as varchar(1000) ) as cust_first_nm					
	  ,cast( substr( last_name, 1, 1000 ) as varchar(1000) ) as cust_last_nm					
	  ,cast( substr( contact_number, 1, 1000 ) as varchar(1000) ) as cust_contact_num				
	  ,cast( substr( mobile_number, 1, 1000 ) as varchar(1000) ) as cust_mobile_num				
	  ,cast( substr( email, 1, 1000 ) as varchar(1000) ) as cust_email						
	  ,cast( substr( membership_number, 1, 1000 ) as varchar(1000) ) as cust_membership_num			
	  ,cast( substr( loyalty_scheme, 1, 1000 ) as varchar(1000) ) as loyalty_scheme_nm				
	  ,cast( case when allow_email = 'true' then 'Y' else 'N' end as varchar(1) ) as allow_email_flg					
	  ,cast( case when is_guest = 'true' then 'Y' else 'N' end as varchar(1) ) as is_guest_flg				
	  ,cast( substr( ip_address,1, 100 ) as varchar(100) ) as ip_addr					  
      ,load_dttm
    from stg.kafka_order_event_strm
    where ( order_id, load_dttm ) in ( select order_id, max( load_dttm ) from stg.kafka_order_event_strm group by 1 )
),
loc_key as (
    select location_key, location_id
    --from rpt.location_dim_curr_v
    from {{ ref( 'location_dim' ) }}
    where dbt_valid_to is null 
),
ord_key as (
    select order_key, order_id
    --from dw.order_key_xref
    from {{ ref( 'order_key_xref' ) }}
)  

select o.order_key, l.location_key, n.order_dt, n.order_id, n.basket_id, n.group_order_id, n.order_guid, n.external_ref_cd, n.api_client_ref_cd
	,n.pos_ref_cd, n.order_status_cd, n.order_placed_utc_dttm, n.order_placed_local_dttm, n.order_ready_utc_dttm, n.order_closed_utc_dttm
	,n.order_closed_local_dttm, n.order_cancelled_utc_dttm, n.order_failed_utc_dttm, n.order_prep_end_time_utc_dttm, n.order_prep_end_time_local_dttm
	,n.order_time_start_making_utc_dttm, n.order_time_start_making_local_dttm, n.order_time_fired_utc_dttm, n.order_time_fired_local_dttm
	,n.order_time_ack_utc_dttm, n.order_time_ack_local_dttm, n.order_sched_fire_time_utc_dttm, n.order_sched_fire_time_local_dttm, n.time_estimate_min_qty
	,n.time_wanted_utc_dttm, n.time_wanted_local_dttm, n.start_time_utc_dttm, n.start_time_local_dttm, n.utc_offset_num, n.julian_create_ts
	,n.julian_ts_offset, n.sched_flg, n.adv_flg, n.cancel_reason_cd, n.location_external_ref_cd, n.loyalty_reward_id, n.transmisison_method_cd
	,n.price_chg_at_fire_time_flg, n.sched_order_price_chg_mode_cd, n.coupon_cd, n.sub_total_amt, n.sales_tax_amt, n.tip_amt, n.discount_amt
	,n.net_total_amt, n.total_amt, n.loc_handoff_charge_amt, n.cust_handoff_charge_amt, n.fee_amt, n.vendor_discount_amt, n.vendor_handoff_charge_amt 
	,n.vendor_tax_amt, n.vendor_total_amt, n.origin_nm, n.origin_slug, n.client_platform_nm, n.replaced_order_guid, n.source_fave_num, n.standing_order_template_nm
	,n.origin_source_nm, n.referral_partner_id, n.third_party_referral_cd, n.handoff_method_nm , n.handoff_addr, n.cust_id, n.cust_external_ref_cd
	,n.cust_first_nm, n.cust_last_nm, n.cust_contact_num, n.cust_mobile_num, n.cust_email, n.cust_membership_num, n.loyalty_scheme_nm
	,n.allow_email_flg, n.is_guest_flg, n.ip_addr, n.load_dttm, current_timestamp() as last_update_dttm
from new_ord n
inner join loc_key l
    on n.location_id = l.location_id
inner join ord_key o
    on n.order_id = o.order_id     
 