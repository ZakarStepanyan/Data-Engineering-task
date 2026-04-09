SELECT
    CAST(id, 'Int32') AS activity_type_id,
    trimBoth(name) AS activity_name,
    lower(trimBoth(type)) AS activity_type_key,
    -- Converting Yes/No to UInt8 (1/0)
    multiIf(lower(active) = 'yes', 1, 0) AS is_active
FROM {{ source('crm_source', 'activity_types') }}