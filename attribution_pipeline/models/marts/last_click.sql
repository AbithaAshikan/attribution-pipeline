SELECT *
    FROM (
        SELECT
            user_pseudo_id,         
            source AS last_click_source,
            event_name,
            event_date,
            TO_TIMESTAMP(event_timestamp / 1000000) AS last_click_time,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id
                ORDER BY event_date desc, event_timestamp desc
            ) AS rk
        FROM {{ ref('stg_events') }}
    )
    WHERE rk = 1
