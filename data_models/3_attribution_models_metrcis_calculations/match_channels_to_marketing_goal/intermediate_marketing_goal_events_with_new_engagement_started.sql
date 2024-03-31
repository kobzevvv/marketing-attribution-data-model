{{ config( 
        tags=["models_weights_calculation"],
        materialized='table'
) }}

with

-- input 
    intermediate_marketing_goal_events as (
        select * from {{ref('intermediate_marketing_goal_events')}}
    ),

--
    companies_with_marketing_key_goal_events_array as (
        select 
            prospect_id,

            groupArray(
                (   event_id,
                    event_name,
                    timestamp
                )
            )                                                                           as marketing_key_goal_events_array

        from intermediate_marketing_goal_events
        group by prospect_id
    ),

    marketing_key_goal_events_with_lifecycle_status as (
        with 

            {% set sales_engagment_lifecycle_duration %}
                select param_value
                from gsheet_marketing_attribution_coded_params
                where 
                    param_name = 'sales_engagment_lifecycle_duration'
            {% endset %}

    
            arrayFilter(
                marketing_key_goal_event_tuple -> 
                    marketing_key_goal_event_tuple.3 
                        between timestamp - interval {{sales_engagment_lifecycle_duration}}
                            and timestamp - interval 1 second,

                marketing_key_goal_events_array
            )                                                                           as marketing_key_goal_events_in_the_same_lifecycle_status_array

        select
            length(
                marketing_key_goal_events_in_the_same_lifecycle_status_array
            ) = 0                                                                       as is_new_engagment_started, 

            intermediate_marketing_goal_events.*

        from intermediate_marketing_goal_events
        left join companies_with_marketing_key_goal_events_array
        using prospect_id
    ),

    goals_with_engadgments as (
        select
            prospect_id,
            timestamp                                                              as engadgment_goal_datetime

        from marketing_key_goal_events_with_lifecycle_status
        where is_new_engagment_started
    ),

    look_for_last_engedgment_goal as (
        select 
            all_goals.event_id,
            max(engadgment_goal_datetime)                                               as previos_marketing_goal_to_reset_sources_datetime

        from marketing_key_goal_events_with_lifecycle_status                            as all_goals
        left join goals_with_engadgments
        using prospect_id

        where 
                engadgment_goal_datetime 
            <   all_goals.timestamp - interval 2 day

        group by all_goals.event_id
    ),

    _join_previos_marketing_goal_to_reset_sources_datetime as (
        select *
        from marketing_key_goal_events_with_lifecycle_status
        left join look_for_last_engedgment_goal
        using event_id
    )

-- final
        select * from _join_previos_marketing_goal_to_reset_sources_datetime