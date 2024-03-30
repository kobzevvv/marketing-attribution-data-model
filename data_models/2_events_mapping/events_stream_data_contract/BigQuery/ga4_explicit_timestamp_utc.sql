select
    parse_date ('%Y%m%d', event_date)                   as event_date,
    
    format_timestamp (
        "%Y-%m-%d %H:%M:%S",
        timestamp_micros (event_timestamp)
    )                                                   as timestamp_utc,

    *

from `bigquery-account-id.marketing_attribution.ga4_daily_parts_to_single_dataset`
