{{ config(
        materialized='incremental',
        unique_key='row_id') }}

with order_event as (
    select 
         cast( order_id as number(38) ) as order_id
		 
		,cast( julian_create_timestamp as number(38) ) as julian_create_timestamp
		,cast( julian_timestamp_offset as number(38) ) as julian_timestamp_offset
		,cast( basket_id as number(38) ) as basket_id
		,cast( group_order_id as number(38) ) as group_order_id
		,order_guid
		,external_reference
		,api_client_reference
		,pos_ref
		,order_status
		,cast( time_placed_utc as timestamp_ntz(3) ) as time_placed_utc
		,cast( time_placed_local as timestamp_ntz(3) ) as time_placed_local
		,cast( time_ready_utc as timestamp_ntz(3) ) as time_ready_utc
		,cast( time_closed_utc as timestamp_ntz(3) ) as time_closed_utc
		,cast( time_closed_local as timestamp_ntz(3) ) as time_closed_local
		,cast( time_cancelled_utc as timestamp_ntz(3) ) as time_cancelled_utc
		,cast( time_failed_utc as timestamp_ntz(3) ) as time_failed_utc
		,cast( prep_end_time_utc as timestamp_ntz(3) ) as prep_end_time_utc
		,cast( prep_end_time_local as timestamp_ntz(3) ) as prep_end_time_local
		,cast( time_start_making_utc as timestamp_ntz(3) ) as time_start_making_utc
		,cast( time_start_making_local as timestamp_ntz(3) ) as time_start_making_local
		,cast( time_fired_utc as timestamp_ntz(3) ) as time_fired_utc
		,cast( time_fired_local as timestamp_ntz(3) ) as time_fired_local
		,cast( time_acknowledged_utc as timestamp_ntz(3) ) as time_acknowledged_utc
		,cast( time_acknowledge_local as timestamp_ntz(3) ) as time_acknowledge_local
		,cast( scheduled_fire_time_utc as timestamp_ntz(3) ) as scheduled_fire_time_utc
		,cast( scheduled_fire_time_local as timestamp_ntz(3) ) as scheduled_fire_time_local
		,cast( utc_offset as number(4,2) ) as utc_offset
		,was_scheduled
		,is_advanced
		,cancel_reason
		,cast( location_id as number(38) ) as location_id
		,location_name
		,cast( location_utc_offset as number(4,2) ) as location_utc_offset
		,location_latitude
		,location_longitude
		,external_ref
		,cast( brand_id as number(38) ) as brand_id
		,brand_guid
		,brand_name
		,loyalty_reward_id
		,transmisison_method
		,has_price_changed_at_fire_time
		,scheduled_order_price_change_mode
		,coupon
		,cast( sub_total as number(12,2) ) as sub_total
		,cast( sales_tax as number(12,2) ) as sales_tax
		,cast( tip as number(12,2) ) as tip
		,cast( location_handoff_charge as number(12,2) ) as location_handoff_charge
		,cast( discount as number(12,2) ) as discount
		,cast( total as number(12,2) ) as total
		,cast( net_total as number(12,2) ) as net_total
		,cast( customer_handoff_charge as number(12,2) ) as customer_handoff_charge
		,cast( fees as number(12,2) ) as fees
		,cast( vendor_discount as number(12,2) ) as vendor_discount
		,cast( vendor_handoff_charge as number(12,2) ) as vendor_handoff_charge
		,cast( vendor_tax as number(12,2) ) as vendor_tax
		,cast( vendor_total as number(12,2) ) as vendor_total
		,origin_name
		,origin_slug
		,client_platform
		,replaced_order_guid
		,cast( source_fave as number(38) ) as source_fave
		,standing_order_template
		,origin_source
		,cast( referral_partner_id as number(38) ) as referral_partner_id
		,third_party_referral
		,handoff_method
		,handoff_address
		,cast( time_estimate as number(38) ) as time_estimate
		,cast( time_wanted_utc as timestamp_ntz(3) ) as time_wanted_utc
		,cast( time_wanted_local as timestamp_ntz(3) ) as time_wanted_local
		,cast( start_time_utc as timestamp_ntz(3) ) as start_time_utc
		,cast( start_time_local as timestamp_ntz(3) ) as start_time_local
		,cast( customer_id as number(38) ) as customer_id
		,customer_external_reference
		,first_name
		,last_name
		,contact_number
		,mobile_number
		,email
		,membership_number
		,loyalty_scheme
		,allow_email
		,is_guest
		,ip_address

		,cast( headers as variant ) as headers
		,cast( deliveries as variant ) as deliveries
		,cast( handoff_custom_fields as variant ) as handoff_custom_fields
		,cast( login_providers as variant ) as login_providers
		,cast( custom_fees as variant ) as custom_fees
		,cast( handoff_events as variant ) as handoff_events
		
        ,load_dttm
        ,action
        ,isupdate as is_update
        ,row_id
    from raw.kafka_order_event_strm_v
)

select *
from order_event