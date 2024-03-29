{{ config( 
		tags=["models_weights_calculation"],
        materialized='table'
) }}

with
-- input
    intermediate_marketing_and_sales_goals_events_with_channels_array as (
        select * 
		from {{ref('intermediate_marketing_and_sales_goals_events_with_channels_array')}}
		where length(traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array) > 0
	),

    intermediate_marketing_channels_events as (
        select * from {{ref('intermediate_marketing_channels_events')}}
    ),

--
	-- bringing the event name to the naming convention of column names in sql
		replaceRegexpAll(
			lower(goal_event_name), 
			'[ -/()]',
			'_'
		)																				as event_sql_name,

	-- first touch and last touch 
		(	matched_marketing_goal_event_datetime,
			1,
            goal_value,
            goal_event_id
		) 																				as single_touch_traffic_source_event_datetime_weight_and_goal_value_tuple,

    -- and u model
		(	matched_marketing_goal_event_datetime,
			if(
				length(traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array)=1,
				1,

				0.5	
			),

            goal_value,
            goal_event_id
		) 																				as u_model_traffic_source_event_datetime_weight_and_goal_value_tuple,


		traffic_source_event_with_contribution_as_a_first_touch as (
			select
				traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array[1].2  	as traffic_source_event_id,

				[	(	event_sql_name || '_first_touch' || '_by_company',
						single_touch_traffic_source_event_datetime_weight_and_goal_value_tuple
					),

					(	event_sql_name || '_u_model' || '_by_company',
						u_model_traffic_source_event_datetime_weight_and_goal_value_tuple
					)
				]																		as goals_with_params_array

			from intermediate_marketing_and_sales_goals_events_with_channels_array

		),

		traffic_source_event_with_contribution_as_a_last_touch as (
			select
				traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array[-1].2 	as traffic_source_event_id,

				[	(	event_sql_name || '_last_touch' || '_by_company',
						single_touch_traffic_source_event_datetime_weight_and_goal_value_tuple
					),

					(	event_sql_name || '_u_model' || '_by_company',
						u_model_traffic_source_event_datetime_weight_and_goal_value_tuple
					)
				]																		as goals_with_params_array

			from intermediate_marketing_and_sales_goals_events_with_channels_array
		),

	-- linear traffic source attribution model
		event_id_and_goal_event_name as (
			select  
				arrayJoin(
					traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array.2
				) 																	  	as traffic_source_event_id,
				
				goal_event_name,

                (   goal_event_datetime,

                        1 
                    / 
                        length(traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array),

                    goal_value,
                    goal_event_id
                )                                                                       as goal_tuple
                	
			from intermediate_marketing_and_sales_goals_events_with_channels_array
		),

		traffic_source_event_id_with_array_of_weights as (
			select 
				traffic_source_event_id,
			
				groupArray(
					(	event_sql_name || '_linear' || '_by_company',
						goal_tuple
					)
				)																		as goals_with_params_array

			from event_id_and_goal_event_name
			group by traffic_source_event_id
		),

	-- group all attributes models to single array
	traffic_source_event_and_attribute_logic_union_all_models as (
			select *
			from traffic_source_event_with_contribution_as_a_first_touch
		union all
			select *
			from traffic_source_event_with_contribution_as_a_last_touch
		union all 
			select *
			from traffic_source_event_id_with_array_of_weights
	),

    _calculate_accolumitive_snapshot as (
        with
            arrayJoin(goals_with_params_array)                                          as goal_params_tuple,

            goal_params_tuple.1                                                         as goal_name_with_attribution_model,
            goal_params_tuple.2.1                                                       as goal_datetime,
            goal_params_tuple.2.2                                                       as goal_weight,
            goal_params_tuple.2.3                                                       as goal_value,
            goal_params_tuple.2.4                                                       as goal_event_id

        select 
            traffic_source_event_id,
            goal_name_with_attribution_model,
            min(goal_datetime)                                                          as first_goal_datetime,
            max(goal_weight)                                                            as max_goal_weight,

            sum( 
                    goal_weight 
                *   goal_value 
            )                                                                           as commulative_value_weighted,

            argMin(
                goal_value,
                goal_datetime
            )                                                                           as first_goal_value,

            sum(goal_value)                                                             as sum_touched_value,
            groupArray(goal_event_id)                                                   as goal_event_ids_array


        from traffic_source_event_and_attribute_logic_union_all_models
        group by 
            traffic_source_event_id,
            goal_name_with_attribution_model
    ),

	traffic_source_event_group_all_models_to_subgle_array_column as (
		with
			groupArray(
                (   goal_name_with_attribution_model,

                    (   first_goal_datetime,
                        max_goal_weight,
                        sum_touched_value,
                        commulative_value_weighted,
                        first_goal_value,
                        goal_event_ids_array
                    ) 

                )
            )                                                                           as goals_with_params_array

		select
			traffic_source_event_id 													as event_id,

            cast(   (	goals_with_params_array.1,
                        goals_with_params_array.2
                    )
                as  
                    Map(
                        LowCardinality(String),

                        Tuple(   
                            DateTime,
                            Float32,
                            Float32,
                            Float32,
                            Float32,
                            Array(UInt64)
                        )
                    )
            )                                                                         as goal_datetime_and_contribution_weight_map

		from _calculate_accolumitive_snapshot
		group by traffic_source_event_id
	),

    _add_traffic_source_hash as (
        select 
            traffic_source_event_group_all_models_to_subgle_array_column.*,
            intermediate_marketing_channels_events.traffic_source_hash

        from traffic_source_event_group_all_models_to_subgle_array_column
        left join intermediate_marketing_channels_events
        using event_id
    )
	
-- final
		select * from _add_traffic_source_hash