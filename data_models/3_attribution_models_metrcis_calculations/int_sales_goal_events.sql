{{ config( tags=["models_weights_calculation"],
         materialized='ephemeral',
         schema = generate_schema_name(var("custom_schema")) ) }}

with

-- input 
    intermediate_activity_events_with_identified_email_materialized as (
        select * from {{ref('dim_activity_events')}}
    ),

    -- params
        (           case_id != 'domain.com'
            and     not match(case_id, '^\\d+\\.\\d+$')
            and     position(case_id,'.') > 0
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
            toLowCardinality(case_id)                                                   as prospect_id,

            toLowCardinality(
                attributes_map['contact_email']
            )                                                                           as contact_email,

            toLowCardinality(event_name)                                                as event_name,

            event_id,

            toFloat32OrZero(
                attributes_map['expected_revenue']
            )                                                                           as expected_revenue

        from web_visit_events__with_company_info

        limit 1 by 
            prospect_id,
            event_name,
            toDate(event_datetime),
            toUInt32(expected_revenue)
    )

    -- final
        select * from events_with_granula_and_sources_params_extracted
        

