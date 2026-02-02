{{ config(
    materialized='incremental', 
    unique_key='user_pseudo_id'
) }}

SELECT *
    FROM (
        SELECT
            user_pseudo_id,         
            source AS first_click_source,
            event_ts_utc AS first_click_time,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id
                ORDER BY event_ts_utc ASC
            ) AS rk
        FROM {{ ref('stg_events') }}
    )
    WHERE rk = 1

{% if is_incremental() %}
AND event_ts_utc > (SELECT MAX(first_click_time) FROM {{ this }})
{% endif %}
