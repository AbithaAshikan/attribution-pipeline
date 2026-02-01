SELECT *
    FROM (
        SELECT
            user_pseudo_id,         
            source AS first_click_source,
            event_name,
            event_date,
            TO_TIMESTAMP(event_timestamp / 1000000) AS first_click_time,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id
                ORDER BY event_date ASC, event_timestamp ASC
            ) AS rk
        FROM {{ ref('stg_events') }}
    )
    WHERE rk = 1