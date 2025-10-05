WITH fact_sales_order_succcess AS (
    SELECT
        ip,
        order_id,
        user_id_db,
        time_stamp,
        local_time,
        collection,
        cart_products
    FROM {{source('raw_dataset', 'raw_summary')}}
    WHERE collection = 'checkout_success'
        AND cart_products IS NOT NULL
),

join_raw_ip_location AS (
    SELECT
        rf.*,
        COALESCE(NULLIF(TRIM(l.city), ''), 'Unknown') AS city,
        COALESCE(NULLIF(TRIM(l.region), ''), 'Unknown') AS region,
        COALESCE(NULLIF(TRIM(l.country_long), ''), 'Unknown') AS country_name
    FROM fact_sales_order_succcess rf
    LEFT JOIN {{source('raw_dataset', 'raw_ip_location')}} AS l 
        ON rf.ip = l.ip
    WHERE l.ip IS NOT NULL
        AND TRIM(l.city) IS NOT NULL AND TRIM(l.city) != '' AND TRIM(city) != '-'
        AND TRIM(l.region) IS NOT NULL AND TRIM(l.region) != '' AND TRIM(l.region) != '-'
        AND TRIM(l.country_short) IS NOT NULL AND TRIM(l.country_short) != '' AND TRIM(l.country_short) != '-'
        AND TRIM(l.country_long) IS NOT NULL AND TRIM(l.country_long) != '' AND TRIM(l.country_long) != '-'
),
get_location_key AS (
    SELECT
        *,
        FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(country_name), ''), COALESCE(TRIM(city), ''), COALESCE(TRIM(region), ''))) AS location_key,
    FROM join_raw_ip_location
),


stg_fact_sales_order AS (
    SELECT
        DISTINCT FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(SAFE_CAST(ip AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(order_id AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(product_id AS STRING)), ''))) AS sales_hash_key,
        COALESCE(SAFE_CAST(user_id_db AS INTEGER), -1) AS user_key,
        location_key,
        COALESCE(SAFE_CAST(order_id AS INTEGER), -1) AS order_key,
        CAST(UNIX_SECONDS(CAST(time_stamp AS TIMESTAMP)) AS INTEGER) AS date_key,
        CAST(local_time AS STRING) AS local_time,
        ip AS ip_address,
        COALESCE(cp.product_id, -1) AS product_key,
        SAFE_CAST(cp.amount AS INT64) AS product_quantity,
        COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0) AS product_price,  
        COALESCE(NULLIF(TRIM(cp.currency), ''), 'Unknown') AS product_currency,
        COALESCE(SAFE_CAST(cp.amount AS INT64), 0) * COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0) AS line_total
    FROM get_location_key
    CROSS JOIN UNNEST(cart_products) AS cp
    WHERE cp.product_id IS NOT NULL
)

SELECT
    sales_hash_key,
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
FROM stg_fact_sales_order