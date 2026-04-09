SELECT
    CAST(option_id, 'Int32') AS reason_id,
    option_label AS reason_label,
    -- Add grouping logic for better reporting
    CASE 
        WHEN option_label IN ('Product Mismatch') THEN 'Product'
        WHEN option_label IN ('Pricing Issues', 'Customer Not Ready') THEN 'Market'
        WHEN option_label IN ('Duplicate Entry', 'Unreachable Customer') THEN 'Process'
        ELSE 'Other'
    END AS reason_category
FROM {{ ref('stg_fields') }}
WHERE field_key = 'lost_reason'