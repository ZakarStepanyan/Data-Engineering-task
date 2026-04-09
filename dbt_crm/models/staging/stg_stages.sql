SELECT
    CAST(stage_id, 'Int32') AS stage_id,
    trimBoth(stage_name) AS stage_name
FROM {{ source('crm_source', 'stages') }}