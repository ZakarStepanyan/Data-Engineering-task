WITH user_raw AS (
    -- Step 1: Pull from the staging model we just created
    SELECT
        user_id,
        user_name,
        email,
        modified_at AS valid_from
    FROM {{ ref('stg_users') }}
),

user_timeline AS (
    -- Step 2: Calculate the timeline boundaries
    SELECT
        *,
        -- Standard lead to find the start of the next version
        lead(valid_from) OVER (PARTITION BY user_id ORDER BY valid_from) AS next_change_time,
        -- Use row_number DESC to guarantee is_current = 1 on the latest record
        row_number() OVER (PARTITION BY user_id ORDER BY valid_from DESC) AS latest_rank
    FROM user_raw
)

SELECT
    -- Deterministic Surrogate Key for version tracking
    cityHash64(user_id, valid_from) AS user_snapshot_sk,

    -- Natural Keys & Attributes
    user_id,
    user_name,
    email,
    
    -- SCD 2 Metadata
    valid_from,
    -- Current records have NULL valid_to; historical records are closed by the next version's start
    if(latest_rank = 1, NULL, next_change_time) AS valid_to,
    if(latest_rank = 1, 1, 0) AS is_current

FROM user_timeline