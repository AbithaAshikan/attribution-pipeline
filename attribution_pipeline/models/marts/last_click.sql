{{ config (
    materialized = 'incremental',
    unique_key = 'user_pseudo_id'
)}}
SELECT *
    FROM (
        SELECT
            user_pseudo_id,         
            source AS last_click_source,
            event_ts_utc AS last_click_time,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id
                ORDER BY event_ts_utc desc
            ) AS rk
        FROM {{ ref('stg_events') }}
    )
    WHERE rk = 1

{% if is_incremental() %}}
and last_click_time > (select MAX(last_click_time) from {{this}})
{% endif %}