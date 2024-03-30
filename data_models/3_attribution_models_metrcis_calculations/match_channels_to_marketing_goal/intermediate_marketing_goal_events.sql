{{ config( 
        tags=["models_weights_calculation"],
        materialized='ephemeral'
) }}

with

-- input 
    intermediate_activity_events_with_identified_email_materialized as (
        select * from {{ref('activity_stream')}}
    ),

    gsheet_marketing_attribution_params as (
        select * from ref('gsheet_marketing_attribution_params')
    ),

    gsheet_marketing_and_sales_goals as (
        select * from ref('gsheet_marketing_and_sales_goals')
    ),
   
--
    web_visit_events__with_company_info as (

        {% set condition_to_exclude_test_and_noise_prospects %}
            select param_value
            from gsheet_marketing_attribution_params
            where 
                param_name = 'condition_to_exclude_test_and_noise_prospects'
        {% endset %}

        (   select goal_name
            from gsheet_marketing_and_sales_goals
            where is_marketing_goal
        )                                                                               as marketing_goals_array

        select *
        from intermediate_activity_events_with_identified_email_materialized
        
        where       
                {{condition_to_exclude_test_and_noise_prospects}}
            and     
                event_name in  marketing_goals_array
    ),

    events_with_granula_and_sources_params_extracted as (
        select 
            timestamp,
            toLowCardinality(prospect_id)                                               as prospect_id,
            toLowCardinality(event_name)                                                as event_name,
            event_id,

            toFloat32OrZero(
                features_map['goal_value']
            )                                                                           as goal_value

        from web_visit_events__with_company_info

        limit 1 by 
            prospect_id,
            timestamp
    )

    -- final
        select *
        from events_with_granula_and_sources_params_extracted
        

