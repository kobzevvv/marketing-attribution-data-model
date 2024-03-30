{{ config( 
        tags=["models_weights_calculation"],
        materialized='table'
) }}

with

-- input
    intermediate_marketing_goal_events_with_marketing_channel_events_array as (
        select * from {{ref('intermediate_marketing_goal_events_with_marketing_channel_events_array')}}
    ),

    intermediate_sales_goal_events_with_matched_marketing_goal as (
        select * from {{ref('intermediate_sales_goal_events_with_matched_marketing_goal')}}
    ),

    sales_goal_event_with_matched_marketing_goal as (
        select
            prospect_id,
            event_id                                                                    as goal_event_id,
            event_name                                                                  as goal_event_name,
            timestamp                                                              as goal_timestamp,

            sales_events.goal_value,

            matched_marketing_goal_event_id,
            marketing_goal_event_name                                                   as matched_marketing_goal_event_name,
            marketing_goal_timestamp                                               as matched_marketing_goal_timestamp,
            attribution_window_started_datetime,
            is_new_engagment_started,
            traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array

        from intermediate_sales_goal_events_with_matched_marketing_goal as sales_events
        left join intermediate_marketing_goal_events_with_marketing_channel_events_array

        on 
                matched_marketing_goal_event_id 
            =   marketing_goal_event_id
    ),


    marketing_goals_mapped_to_union as (
        select
            prospect_id,
            marketing_goal_event_id                                                     as goal_event_id,
            marketing_goal_event_name                                                   as goal_event_name,
            marketing_goal_timestamp                                               as goal_timestamp,

            goal_value,

            marketing_goal_event_id                                                     as matched_marketing_goal_event_id,

            marketing_goal_event_name                                                   as matched_marketing_goal_event_name,
            marketing_goal_timestamp                                               as matched_marketing_goal_timestamp,
            attribution_window_started_datetime,
            is_new_engagment_started,
            traffic_sources_events_in_lookback_window_datetime_id_and_is_direct_array


        from intermediate_marketing_goal_events_with_marketing_channel_events_array
    ),

    union_sales_and_marketing_goals_events as (
            select *
            from sales_goal_event_with_matched_marketing_goal

        union all

            select * 
            from marketing_goals_mapped_to_union
    ),

    _deduplication as (
        select *
        from union_sales_and_marketing_goals_events
        limit 1 by goal_event_id
    )

-- final
    select * from _deduplication

