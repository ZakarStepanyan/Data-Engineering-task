WITH sparse_pivoted AS (
    SELECT
        dc.deal_id,
        dc.change_time AS valid_from,
        dc.field_key,

        if(dc.field_key = 'stage_id', CAST(dc.change_value, 'Int32'), NULL) AS stage_id_event,
        if(dc.field_key = 'user_id', CAST(dc.change_value, 'Int32'), NULL) AS user_id_event,
        if(dc.field_key = 'lost_reason', CAST(dc.change_value, 'Int32'), NULL) AS lost_reason_id_event,
        if(dc.field_key = 'add_time', parseDateTimeBestEffort(dc.change_value), NULL) AS add_time_event
    FROM {{ ref('stg_deal_changes') }} AS dc
),

scd_timeline AS (
    SELECT
        *,
        -- Establish the interval
        lead(valid_from) OVER (PARTITION BY deal_id ORDER BY valid_from) AS next_valid_from,
        -- Correct is_current logic
        row_number() OVER (PARTITION BY deal_id ORDER BY valid_from DESC) AS rec_rank
    FROM sparse_pivoted
)

SELECT
    cityHash64(deal_id, valid_from) AS deal_snapshot_sk,
    deal_id,
    field_key,
    add_time_event,
    stage_id_event,
    user_id_event,
    lost_reason_id_event,
    valid_from,
    if(rec_rank = 1, NULL, next_valid_from) AS valid_to,
    if(rec_rank = 1, 1, 0) AS is_current
FROM scd_timeline