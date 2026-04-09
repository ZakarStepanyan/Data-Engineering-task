SELECT
    CAST(deal_id, 'Int64') AS deal_id,
    parseDateTimeBestEffort(change_time) AS change_time,
    lower(changed_field_key) AS field_key,
    trimBoth(new_value) AS change_value
FROM {{ source('crm_source', 'deal_changes') }}
