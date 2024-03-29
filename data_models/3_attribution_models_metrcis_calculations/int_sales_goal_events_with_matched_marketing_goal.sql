{{ config( tags=["models_weights_calculation"],
         materialized='table',
         schema = generate_schema_name(var("custom_schema")) ) }}

with
-- input 
    intermediate_sales_goal_events as (
        select * from {{ref('intermediate_sales_goal_events')}}
    ),

    intermediate_marketing_goal_events_with_marketing_channel_events_array as (
        select * from {{ref('intermediate_marketing_goal_events_with_marketing_channel_events_array')}}
    ),

--
    sales_key_goal_events_with_matched_marketing_key_goal as (
        with
            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_event_datetime,

                        marketing_goal_event_datetime 
                    <=   sales_goals.event_datetime
                and
                    if_exist_paid_traffic_source
            )                                                                           as event_id_with_paid_traffic_source,

            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_event_datetime,

                        marketing_goal_event_datetime 
                    <=   sales_goals.event_datetime
                and
                    if_exist_non_direct_traffic_source
            )                                                                           as event_id_with_non_direct_traffic_source,

            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_event_datetime,

                        marketing_goal_event_datetime 
                    <=   sales_goals.event_datetime
                and
                    traffic_source_events_count > 0
            )                                                                           as event_id_with_not_zero_traffic_source,
           
            argMaxIf(
                marketing_goal_event_id,
                marketing_goal_event_datetime,

                    marketing_goal_event_datetime 
                <=   sales_goals.event_datetime
            )                                                                           as event_id_with_any_traffic_source_events

        select
            sales_goals.event_id,

            coalesce(
                event_id_with_paid_traffic_source,
                event_id_with_non_direct_traffic_source,
                event_id_with_not_zero_traffic_source,
                event_id_with_any_traffic_source_events
            )                                                                           as matched_marketing_goal_event_id   

        from intermediate_sales_goal_events                                                  as sales_goals
        join intermediate_marketing_goal_events_with_marketing_channel_events_array           as marketing_goals
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

    



