SELECT
    CAST(s.stage_id, 'Int32') AS stage_id,
    -- If the stages table has label, use it. Otherwise, use it from JSON.
    COALESCE(s.stage_name, f.option_label) AS stage_name
FROM {{ ref('stg_stages') }} s
LEFT JOIN {{ ref('stg_fields') }} f 
    ON f.field_key = 'stage_id' 
    AND f.option_id = CAST(s.stage_id, 'String')