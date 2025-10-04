WITH UniqueUsers AS (
    SELECT 
        COALESCE(SAFE_CAST(user_id_db AS INTEGER), -1) AS user_key,
        COALESCE(TRIM(device_id), 'Unknown') as device_id,
        COALESCE(TRIM(email_address), 'Unknown') as email_address,
        COALESCE(TRIM(user_agent), 'Unknown') as user_agent,
        ROW_NUMBER() OVER (PARTITION BY user_id_db ORDER BY email_address) AS rn
    FROM
        {{source('raw_dataset', 'raw_summary')}}
    -- WHERE
    --     TRIM(device_id) IS NOT NULL AND TRIM(device_id) != '' AND TRIM(device_id) != '-'
    --     AND TRIM(email_address) IS NOT NULL AND TRIM(email_address) != '' AND TRIM(email_address) != '-'
    --     AND TRIM(user_agent) IS NOT NULL AND TRIM(user_agent) != '' AND TRIM(user_agent) != '-'
)
SELECT
    user_key,
    device_id,
    email_address,
    user_agent
FROM 
    UniqueUsers
WHERE
    rn = 1