-- Standardizing to lowercase prevents duplicates caused by case sensitivity (e.g., User@Dev.com vs user@dev.com).

SELECT
    CAST(id, 'Int64') AS user_id,
    trimBoth(name) AS user_name,
    lower(trimBoth(email)) AS email,
    -- Using parseDateTimeBestEffort for flexibility with CRM formats
    parseDateTimeBestEffort(modified) AS modified_at,
    -- Add a source system flag if you eventually pull from multiple CRMs
    'crm_system' AS source_system
FROM {{ source('crm_source', 'users') }}
-- Filter out test accounts or incomplete records here
WHERE user_id IS NOT NULL 
  AND user_name != ''