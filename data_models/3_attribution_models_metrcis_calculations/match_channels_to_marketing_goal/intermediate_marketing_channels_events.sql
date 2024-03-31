{{ config( 
        tags=["models_weights_calculation"],
        materialized='ephemeral'
) }}

with
-- input 
    intermediate_activity_events_with_identified_email_materialized as (
        select * from {{ref('activity_stream')}}
    ),

    gsheet_marketing_channel_features as (
        select marketing_channel_features from ref('gsheet_marketing_channel_features')
    ),

    gsheet_marketing_attribution_coded_params as (
        select * from ref('gsheet_marketing_attribution_coded_params')
    ),

    web_visit_events__with_company_info as (

        {% set condition_to_exclude_test_and_noise_prospects %}
            select param_value
            from gsheet_marketing_attribution_coded_params
            where 
                param_name = 'condition_to_exclude_test_and_noise_prospects'
        {% endset %}

        select *
        from intermediate_activity_events_with_identified_email_materialized
        where ({{condition_to_exclude_test_and_noise_prospects}}) 
    ),

    events_with_granula_and_sources_params_extracted as (
        with 
            length(traffic_source_features_map) > 0                                     as if_event_have_any_marketing_channel_info,

            (   select marketing_channel_features
                from gsheet_marketing_channel_features
            )                                                                           as traffic_source_attributes_names_array

        select 
            timestamp,
            prospect_id                                                                 as prospect_id,
            features_map['contact_email']                                               as contact_email,
            event_id,
            
            mapFilter(
                (key,value) -> 
                            key in traffic_source_attributes_names_array
                    and     value !='',

                features_map
            )                                                                           as traffic_source_features_map,

            event_name

        from web_visit_events__with_company_info
        where if_event_have_any_marketing_channel_info
    ),

    events_filter_distinct_source_info as (
        with 
            mapSort(
                ( key,value ) -> key,
                traffic_source_features_map
            )                                                                           as traffic_source_features_map_sorted,

            cityHash64(
                prospect_id,
                contact_email,
                toStartOfQuarter(timestamp),
                traffic_source_features_map_sorted
            )                                                                           as traffic_source_hash

            {% set is_paid_channel_definition %}
                select param_value
                from gsheet_marketing_attribution_coded_params
                where 
                    param_name = 'is_paid_channel_definition'
            {% endset %}

            {% set is_paid_channel_definition %}
                select param_value
                from gsheet_marketing_attribution_coded_params
                where 
                    param_name = 'is_paid_channel_definition'
            {% endset %}

        select 
            traffic_source_hash,

            ({{is_paid_channel_definition}})                                            as is_paid_channel,
            ({{is_direct_channel_definition}})                                          as is_direct_channel, 

            *

        from events_with_granula_and_sources_params_extracted
        limit 1 by traffic_source_hash
    )

-- final
        select * from events_filter_distinct_source_info
        

