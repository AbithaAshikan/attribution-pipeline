{{ config(
    materailized = "incremental",
    unique_key = "user_pseudo_id"
)}}

SELECT
    f.user_pseudo_id,
    f.first_click_source,
    f.first_click_time,
    l.last_click_source,
    l.last_click_time
FROM {{ ref('first_click') }} f
JOIN {{ ref('last_click') }} l
    ON f.user_pseudo_id = l.user_pseudo_id

{% if is_incremental() %}
where f.first_click_time > (select max(f.fast_click_time) from {{ this }})
{% endif %}

