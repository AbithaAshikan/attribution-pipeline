select
event_date,
event_timestamp,
TO_TIMESTAMP(event_timestamp / 1000000) AT TIME ZONE 'UTC' AS event_ts_utc,
event_name,
user_pseudo_id,
"traffic_source.source" as source,
"traffic_source.medium" as medium,
"traffic_source.name" as campaign
from {{ source('ga4', 'ga4_event') }}