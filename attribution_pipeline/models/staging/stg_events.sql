select
event_date,
event_timestamp,
event_name,
user_pseudo_id,
"traffic_source.source" as source,
"traffic_source.medium" as medium,
"traffic_source.name" as campaign
from {{ source('ga4', 'ga4_event') }}