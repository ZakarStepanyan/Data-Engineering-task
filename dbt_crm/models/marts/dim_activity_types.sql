SELECT
    activity_type_id,
    activity_name,
    activity_type_key,
    is_active
FROM {{ ref('stg_activity_types') }}