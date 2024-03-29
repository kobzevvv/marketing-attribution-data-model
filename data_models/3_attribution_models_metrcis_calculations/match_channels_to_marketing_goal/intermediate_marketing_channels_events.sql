{{ config( tags=["models_weights_calculation"],
         materialized='ephemeral',
         schema = generate_schema_name(var("custom_schema")) ) }}

with
-- input 
    intermediate_activity_events_with_identified_email_materialized as (
        select * from {{ref('activity_stream')}}
    ),




    -- params
        (           case_id != 'domain.com'
            and     not match(case_id, '^\\d+\\.\\d+$')
            and     position(case_id,'.') > 0
        )                                                                               as is_case_id_equal_prospect_prospect_id,

        [ 
            'traffic_source_medium',
            'traffic_source_name',
            'session_channel',
            'session_source',
            'session_campaign',
            --'page_referrer',
            'traffic_source_source',
            'session_term',
            'session_content'
        ]                                                                               as traffic_source_attributes_names_array,


    web_visit_events__with_company_info as (
        select *
        from intermediate_activity_events_with_identified_email_materialized
        
        where       
                    is_case_id_equal_prospect_prospect_id
    ),

    events_with_granula_and_sources_params_extracted as (
        with 
            length(traffic_source_attributes_map) > 0                                   as if_event_have_any_marketing_channel_info

        select 
            event_datetime,
            case_id                                                                     as prospect_id,
            attributes_map['contact_email']                                             as contact_email,
            event_id,
            
            mapFilter(
                (key,value) -> 
                            key in traffic_source_attributes_names_array
                    and     value !='',

                attributes_map
            )                                                                           as traffic_source_attributes_map,

            event_name

        from web_visit_events__with_company_info
        where if_event_have_any_marketing_channel_info
    ),

    events_filter_distinct_source_info as (
        with 
            mapSort(
                ( key,value ) -> key,
                traffic_source_attributes_map
            )                                                                           as traffic_source_attributes_map_sorted,

            cityHash64(
                prospect_id,
                contact_email,
                toStartOfQuarter(event_datetime),
                traffic_source_attributes_map_sorted
            )                                                                           as traffic_source_hash



        select 
            traffic_source_hash,

                traffic_source_attributes_map['marketing_channel'] ilike '%paid%'
            or  traffic_source_attributes_map['marketing_channel'] = 'ABM'
            or  traffic_source_attributes_map['marketing_channel'] ilike '%Ads%'            as is_paid_channel,

            --
                traffic_source_attributes_map['session_channel'] 
            in  
                [   'direct',
                    'offline'
                ]                                                                       as is_direct_channel, 

            *

        from events_with_granula_and_sources_params_extracted
        limit 1 by traffic_source_hash
    )

-- final
        select * from events_filter_distinct_source_info
        

