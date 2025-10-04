SELECT
    DISTINCT sales_hash_key,
    product_key,
    user_key,
    location_key,
    date_key,
    order_key,
    local_time,
    ip_address,
    product_quantity,
    product_price,
    product_currency,
    line_total
FROM {{ ref('stg_fact_sales_order') }}