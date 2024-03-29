{{ config( 
        tags=["models_weights_calculation"],
        materialized='table'
) }}

with 

-- input 
    intermediate_marketing_channels_events as (
        select * from {{ref('intermediate_marketing_channels_events')}}
    ),

-- 
    _marketing_channel_events_deduplicated as (
        select *
        from intermediate_marketing_channels_events

        limit 1 by 
            prospect_id,
            event_id
    ),

    prospects_with_events_array as (
        with 
            groupArray(
                (   event_datetime,
                    event_id,
                    is_direct_channel,
                    is_paid_channel
                )
            )                                                                           as events_tuples_array_not_ordered

        select 
            prospect_id,

            arraySort(
                event_tuple -> event_tuple.1                                            as nested_event_datetime,
                events_tuples_array_not_ordered
            )                                                                           as traffic_source_event_datetime_id_and_is_direct_array
            
        from _marketing_channel_events_deduplicated
        group by prospect_id
    )

-- final
        select * from prospects_with_events_array

