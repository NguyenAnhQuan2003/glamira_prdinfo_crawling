WITH UniqueProducts AS (
    SELECT
        DISTINCT
        COALESCE(product_id, -1) AS product_key,
        COALESCE(NULLIF(TRIM(SAFE_CAST(name AS STRING)), ''), NULLIF(TRIM(SAFE_CAST(name AS STRING)), '-'), 'Unknown') AS product_name,
        COALESCE(NULLIF(TRIM(category_name), ''), 'Unknown')AS category_name,
        COALESCE(NULLIF(TRIM(collection), ''), 'Unknown') AS collection_name,
        COALESCE(NULLIF(TRIM(product_type), ''), 'Unknown') AS product_type,
        COALESCE(NULLIF(TRIM(gender), ''), 'Unknown') AS product_gender
    FROM
        {{source('raw_dataset', 'raw_products')}}
)
SELECT
    product_key,
    product_name,
    category_name,
    collection_name,
    product_type,
    product_gender
FROM UniqueProducts