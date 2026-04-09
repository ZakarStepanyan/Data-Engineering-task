SELECT DISTINCT
    id,
    field_key,
    field_display_name
FROM {{ ref('stg_fields') }}