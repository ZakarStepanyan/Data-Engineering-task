WITH enriched_metrics AS (
    -- Step 1: Calculate running counts and binary flags
    SELECT
        *,
        -- Running count of how many times stage_id_event was NOT NULL
        count(stage_id_event) OVER (PARTITION BY deal_id ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS changed_staged_count,
        -- Running count of how many times user_id_event was NOT NULL
        count(user_id_event) OVER (PARTITION BY deal_id ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS changed_users_count,
        anyLast(add_time_event) OVER (PARTITION BY deal_id ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS deal_created_at,
        -- Logic for Status Flags
        if(lost_reason_id_event IS NOT NULL, 1, 0) AS is_fail,
        -- Assuming 'win' stage or similar logic; adjust ID based on your dim_stages
        if(lost_reason_id_event IS NULL AND stage_id_event = 10, 1, 0) AS is_passed,
        if(lost_reason_id_event IS NULL AND stage_id_event != 10, 1, 0) AS is_inprogress
    FROM {{ ref('dim_deal_history') }}
)

SELECT
    -- Identifiers
    m.deal_snapshot_sk,
    m.deal_id as deal_id,
    f.id as field_id,
    u.user_snapshot_sk as user_snapshot_sk,
    user_id_event,
    s.stage_id as stage_id,
    lr.reason_id as lost_reason_id,

    -- Metrics
    dateDiff('day', m.deal_created_at, m.valid_from) AS day_in_pipeline,
    m.changed_staged_count,
    m.changed_users_count,
    m.is_inprogress,
    m.is_passed,
    m.is_fail,

FROM enriched_metrics m
LEFT JOIN {{ ref('dim_fields') }} f 
    ON m.field_key = f.field_key
LEFT JOIN {{ ref('dim_users') }} u 
    ON m.user_id_event = u.user_id
    -- Match the deal event time to the user's validity window
    AND (
    m.valid_from >= u.valid_from AND m.valid_from < u.valid_to
    OR u.valid_to IS NULL)
LEFT JOIN {{ ref('dim_stages') }} s 
    ON m.stage_id_event = s.stage_id
LEFT JOIN {{ ref('dim_lost_reasons') }} lr 
    ON m.lost_reason_id_event = lr.reason_id