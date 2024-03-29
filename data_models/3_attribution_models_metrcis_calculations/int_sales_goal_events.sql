{{ config( tags=["models_weights_calculation"],
         materialized='ephemeral',
         schema = generate_schema_name(var("custom_schema")) ) }}

with

-- input 
    intermediate_activity_events_with_identified_email_materialized as (
        select * from {{ref('activity_stream')}}
    ),

    -- params
        (           prospect_id != 'domain.com'
            and     not match(prospect_id, '^\\d+\\.\\d+$')
            and     position(prospect_id,'.') > 0
        )                                                                               as is_prospect_id_complient,

    web_visit_events__with_company_info as (
        select *
        from intermediate_activity_events_with_identified_email_materialized
        
        where       
                is_prospect_id_complient
            and     
                event_name in  ['Closed Won']
    ),

    events_with_granula_and_sources_params_extracted as (
        select 
            event_datetime,
            toLowCardinality(prospect_id)                                               as prospect_id,
            toLowCardinality(event_name)                                                as event_name,

            event_id,

            toFloat32OrZero(
                attributes_map['goal_value']
            )                                                                           as goal_value

        from web_visit_events__with_company_info

        limit 1 by 
            prospect_id,
            event_name,
            toDate(event_datetime),
            toUInt32(goal_value)
    )

    -- final
        select * from events_with_granula_and_sources_params_extracted
        

