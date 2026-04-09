WITH raw_fields AS (
    SELECT
        id,
        field_key,
        name AS field_display_name,
        -- If it's empty, we want a single-element array with an empty string 
        -- so the LEFT ARRAY JOIN still keeps the row
        if(field_value_options = '' OR field_value_options IS NULL, 
           ['{}'], 
           JSONExtractArrayRaw(field_value_options)) AS options_array
    FROM {{ source('crm_source', 'fields') }}
)

SELECT
    id,
    field_key,
    field_display_name,
    -- These will return NULL for non-JSON keys, which is correct
    nullIf(JSONExtractString(option_json, 'id'), '') AS option_id,
    nullIf(JSONExtractString(option_json, 'label'), '') AS option_label
FROM raw_fields
LEFT ARRAY JOIN options_array AS option_json