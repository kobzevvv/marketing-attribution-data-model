with 


    filtered_event_params_cte as (
        select

        event_name,

        ARRAY(
            select  
                event_param.value.string_value                                          as string_value

            from UNNEST(event_params) as event_param

            where 
                event_param.key = 'hashed_email'
            and 
                event_param.value.string_value !='null'

        )[SAFE_OFFSET(0)]                                                               as hashed_email,

        ARRAY(
            select  
            as struct 
                ep.key, 
                ep.value.string_value                                                   as string_value

            from UNNEST(event_params) as ep

            where 
                ep.key IN (
                'source',
                'medium', 
                'campaign',
                'term',
                'page_location'
                ) 
            AND 
                ep.value.string_value !='null'
        )                                                                               as filtered_event_params,

        *

    from `bigquery-account-id.analytics_407788275.ga4_explicit_timestamp_utc`
  )

select *
from filtered_event_params_cte
where 
  ARRAY_LENGTH(filtered_event_params) > 0