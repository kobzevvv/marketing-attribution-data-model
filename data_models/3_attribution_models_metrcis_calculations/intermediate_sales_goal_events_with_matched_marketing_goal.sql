{{ config( 
        tags=["models_weights_calculation"],
        materialized='table'
) }}

with
-- input 
    intermediate_sales_goal_events as (
        select * from {{ref('intermediate_sales_goal_events')}}
    ),

    intermediate_marketing_goal_events_with_marketing_channel_events_array as (
        select * from {{ref('intermediate_marketing_goal_events_with_marketing_channel_events_array')}}
    ),

    gsheet_marketing_attribution_coded_params as (
        select * from ref('gsheet_marketing_attribution_coded_params')
    ),

    -- params
        {% set marketing_goal_event_ids_list_by_spriority_to_match_with_sales_goal %}
            select param_value
            from gsheet_marketing_attribution_coded_params
            where 
                param_name = 'marketing_goal_event_id_by_priority_to_match_with_sales_goal'
        {% endset %}

        (   select marketing_channel_features
            from gsheet_marketing_channel_features
        )                                                                               as traffic_source_attributes_names_array,

--
    sales_key_goal_events_with_matched_marketing_key_goal as (
        with
            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_timestamp,

                        marketing_goal_timestamp 
                    <=   sales_goals.timestamp
                and
                    if_exist_paid_traffic_source
            )                                                                           as event_id_with_paid_traffic_source,

            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_timestamp,

                        marketing_goal_timestamp 
                    <=   sales_goals.timestamp
                and
                    if_exist_non_direct_traffic_source
            )                                                                           as event_id_with_non_direct_traffic_source,

            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_timestamp,

                        marketing_goal_timestamp 
                    <=   sales_goals.timestamp
                and
                    traffic_source_events_count > 0
            )                                                                           as event_id_with_not_zero_traffic_source,
           
            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_timestamp,

                    marketing_goal_timestamp 
                <=   sales_goals.timestamp
            )                                                                           as event_id_with_any_traffic_source_events

        select
            sales_goals.event_id,

            coalesce(
                {{marketing_goal_event_ids_list_by_spriority_to_match_with_sales_goal}}
            )                                                                           as matched_marketing_goal_event_id   

        from intermediate_sales_goal_events                                             as sales_goals
        join intermediate_marketing_goal_events_with_marketing_channel_events_array     as marketing_goals
        using prospect_id

        group by sales_goals.event_id
    ),

    _join_back_other_fields as (
        select *
        from sales_key_goal_events_with_matched_marketing_key_goal
        left join intermediate_sales_goal_events
        using event_id
    )

-- final
        select * from _join_back_other_fields

    



