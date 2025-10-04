SELECT
    DISTINCT
    product_key,
    product_name,
    category_name,
    collection_name,
    product_type,
    product_gender
FROM {{ref('stg_raw_products')}}