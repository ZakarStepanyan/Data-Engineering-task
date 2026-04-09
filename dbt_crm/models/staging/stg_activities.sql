-- activity_id is not unqiue, but the rows are unique.
-- Since the activity_id is not used for any logic, do not deduplicte it.

--     ┌─count(activity_id)─┬─activity_id─┐
--  1. │                  2 │      488221 │
--  2. │                  2 │      500358 │
--  3. │                  2 │      283914 │
--  4. │                  2 │      818588 │
--  5. │                  2 │      835226 │
--  6. │                  2 │      521731 │
--  7. │                  2 │      370773 │
--  8. │                  2 │      206894 │
--  9. │                  2 │      283308 │
-- 10. │                  2 │      855539 │
-- 11. │                  2 │      332746 │
--     └────────────────────┴─────────────┘

-- Standardizing Activity records from raw CSV-style strings
SELECT
    -- Casting IDs to Int64 for high-performance joins
    CAST(activity_id, 'Int64') AS activity_id,
    CAST(deal_id, 'Int64') AS deal_id,
    CAST(assigned_to_user, 'Int64') AS user_id,
    
    -- Normalizing the activity label
    lower(trimBoth(type)) AS activity_type_label,
    
    -- Handling the 'True'/'False' strings for ClickHouse
    -- We convert these to 1 and 0 for easier summing in the Fact table
    if(lower(done) = 'true', 1, 0) AS is_done,
    
    -- Parsing the ISO 8601 timestamp (e.g., 2024-07-24T06:47:10)
    parseDateTimeBestEffort(due_to) AS activity_timestamp,
    
    'crm_system' AS source_system
FROM {{ source('crm_source', 'activity') }}
WHERE activity_id IS NOT NULL