WITH BASE AS (
        SELECT
        a.activity_id AS act_id,
        a.deal_id AS act_deal_id,
        a.user_id AS act_user_id,
        a_t.activity_type_id,

        a.activity_timestamp,
        a.is_done,
        u.user_snapshot_sk,
        h.deal_snapshot_sk
    FROM {{ ref('stg_activities') }} AS a
    LEFT JOIN {{ ref('dim_users') }} AS u 
        ON a.user_id = u.user_id 
        AND (
            a.activity_timestamp >= u.valid_from AND a.activity_timestamp < u.valid_to
            OR u.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_deal_history') }} AS h 
        ON a.deal_id = h.deal_id 
        AND a.activity_timestamp >= h.valid_from 
        AND (a.activity_timestamp < h.valid_to OR h.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_activity_types') }} AS a_t
        ON a.activity_type_label = a_t.activity_type_key
)
SELECT
    act_id AS activity_id,
    deal_snapshot_sk,
    act_deal_id AS deal_id,
    user_snapshot_sk,
    act_user_id AS user_id,
    activity_type_id,
    is_done,

    -- Cumulative Metrics using the unique names
    count(*) OVER (
        PARTITION BY act_user_id 
        ORDER BY activity_timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS user_total_activity_count,

    sum(is_done) OVER (
        PARTITION BY act_user_id 
        ORDER BY activity_timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS user_done_activity_count
FROM BASE
ORDER BY user_id, activity_timestamp