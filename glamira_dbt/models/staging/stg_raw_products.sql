WITH UniqueProducts AS (
    SELECT
        DISTINCT
        COALESCE(product_id, -1) AS product_key,
        COALESCE(NULLIF(TRIM(SAFE_CAST(name AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(name AS STRING)), '-'), 'Unknown') AS product_name,
        COALESCE(CAST(SAFE_CAST(price AS FLOAT64) AS FLOAT64), 0.0) AS product_price,
        COALESCE(CAST(SAFE_CAST(category AS INTEGER) AS INTEGER), 0) AS category_id,
        COALESCE(NULLIF(TRIM(SAFE_CAST(category_name AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(category_name AS STRING)), '-'), 'Unknown') AS category_name,
        COALESCE(CAST(SAFE_CAST(collection_id AS INTEGER) AS INTEGER), 0) AS collection_id,
        COALESCE(NULLIF(TRIM(SAFE_CAST(collection AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(collection AS STRING)), '-'), 'Unknown') AS collection_name,
        COALESCE(NULLIF(TRIM(SAFE_CAST(product_type AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(product_type AS STRING)), '-'), 'Unknown') AS product_type,
        COALESCE(NULLIF(TRIM(ARRAY_TO_STRING(SAFE_CAST(visible_contents AS ARRAY<STRING>), '')), ''), NULLIF(TRIM(ARRAY_TO_STRING(SAFE_CAST(visible_contents AS ARRAY<STRING>), '')), '-'), 'Unknown') AS visible_contents,
        COALESCE(NULLIF(TRIM(SAFE_CAST(status AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(status AS STRING)), '-'), 'Unknown') AS product_status
    FROM
        {{source('raw_dataset', 'raw_products')}}
)
SELECT
    product_key,
    product_price,
    product_name,
    category_id,
    category_name,
    collection_id,
    collection_name,
    product_type,
    visible_contents,
    product_status,
FROM UniqueProducts