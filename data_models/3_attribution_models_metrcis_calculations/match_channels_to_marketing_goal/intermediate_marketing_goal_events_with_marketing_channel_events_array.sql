{{ 	config( 
		tags=["models_weights_calculation"],
        materialized='table'
) }}

with 

-- input
	intermediate_marketing_goal_events_with_new_engagement_started as (
		select * from {{ref('intermediate_marketing_goal_events_with_new_engagement_started')}}
	),

	intermediate_prospects_with_marketing_channels_events_array as (
		select * from {{ref('intermediate_prospects_with_marketing_channels_events_array')}}
	),

	marketing_key_goal_events_filtered_new_engedgment_started as (
		select *
		from intermediate_marketing_goal_events_with_new_engagement_started
		where is_new_engagment_started
	),

	marketing_key_goal_events_with_traffic_sources_array as (
		select *
		from marketing_key_goal_events_filtered_new_engedgment_started		
		left join intermediate_prospects_with_marketing_channels_events_array
		using prospect_id
	),

	marketing_key_goal_events_with_traffic_sources_array as (
        with 
            arrayFilter(
				traffic_source_event_tuple ->

					traffic_source_event_tuple.1
						between	attribution_window_started_datetime
							and timestamp,
					
				traffic_source_timestamp_id_and_is_direct_array
			)                                                   						as traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array,

            length(traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array) as traffic_source_events_count,

            length(
                arrayFilter(
                    traffic_source_event_tuple ->
                        traffic_source_event_tuple.3 != 1,

                    traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array
                )
            ) > 0                                                                       as if_exist_non_direct_traffic_source,

            length(
                arrayFilter(
                    traffic_source_event_tuple ->
                        traffic_source_event_tuple.4 = 1,

                    traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array
                )
            ) > 0                                                                       as if_exist_paid_traffic_source,

            {% set attribution_loockback_window %}
                select param_value
                from gsheet_marketing_attribution_coded_params
                where 
                    param_name = 'attribution_loockback_window'
            {% endset %}

			

		select
			prospect_id, 
			
			greatest(
				timestamp - interval {{attribution_loockback_window}},
				previos_marketing_goal_to_reset_sources_datetime + interval 1 second
			)																			as attribution_window_started_datetime,

			traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array,
            traffic_source_events_count,
            if_exist_non_direct_traffic_source,
            if_exist_paid_traffic_source,

			timestamp 																as marketing_goal_timestamp,
			is_new_engagment_started,
			event_name 																	as marketing_goal_event_name,
			event_id 																	as marketing_goal_event_id,
            goal_value
	
		from marketing_key_goal_events_with_traffic_sources_array
	)

-- final
		select * from marketing_key_goal_events_with_traffic_sources_array