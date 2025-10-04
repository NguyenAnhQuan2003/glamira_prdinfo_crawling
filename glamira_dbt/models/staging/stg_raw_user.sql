WITH UniqueUsers AS (
    SELECT
        DISTINCT COALESCE(SAFE_CAST(user_id_db AS INTEGER), -1) AS user_key,
        COALESCE(NULLIF(TRIM(email_address), ''), 'Unknown') AS email_address
    FROM 
        {{source('raw_dataset', 'raw_summary')}}
    WHERE user_id_db IS NOT NULL OR email_address IS NOT NULL
),

CheckDuplicates AS (
    SELECT
        user_key,
        COUNT(*) as duplicate_count
    FROM UniqueUsers
    GROUP BY user_key
    HAVING COUNT(*) > 1
)

SELECT
    uu.user_key,
    uu.email_address
FROM UniqueUsers uu
LEFT JOIN CheckDuplicates cd
    ON uu.user_key = cd.user_key
WHERE cd.user_key IS NULL OR cd.duplicate_count = 1