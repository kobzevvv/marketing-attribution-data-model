-- data preparation to load to clickhouse
-- clickhouse data types are different and we need to cast json as strings

select
    ga4_property_name,
    event_date,
    timestamp_utc,

    event_name,

    TO_JSON_STRING(event_params)                        as event_params_json_string,
    TO_JSON_STRING(device)                              as device_params_json_string,

    user_pseudo_id

from `bigquery-account-id.marketing_attribution.ga4_explicit_timestamp_utc`
