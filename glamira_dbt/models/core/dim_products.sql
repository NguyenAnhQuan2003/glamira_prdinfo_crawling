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
    quantity,
    product_status
FROM {{ref('stg_raw_products')}}