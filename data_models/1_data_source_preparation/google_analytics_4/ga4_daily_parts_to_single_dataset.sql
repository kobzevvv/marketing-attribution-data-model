-- exmaple how you can combine events from different ga4 propersties for all days

    select
        'ga4_property_name_1'                               as ga4_property_name,

        *

    from `bigquery-account-id.analytics_111222333.events_20*`
        -- events_20* is taking all tables like .events_2024_04_27 

union all

    select
        'ga4_property_name_1'                               as ga4_property_name,

        *

    from `bigquery-account-id.analytics_111222334.events_20*`
