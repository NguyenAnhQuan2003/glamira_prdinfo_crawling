WITH rawFilter AS (
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

ipJoin AS (
    SELECT
        rf.*,
        l.location_key
    FROM rawFilter rf
    LEFT JOIN {{ref('dim_ip_location')}} AS l 
        ON rf.ip = l.ip
),


UniqueSalesOrder AS (
    SELECT
        DISTINCT FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(SAFE_CAST(ip AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(order_id AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(product_id AS STRING)), ''))) AS sales_hash_key,
        COALESCE(SAFE_CAST(user_id_db AS INTEGER), -1) AS user_key,
        COALESCE(SAFE_CAST(location_key AS INTEGER), -1) AS location_key,
        COALESCE(SAFE_CAST(order_id AS INTEGER), -1) AS order_key,
        CAST(UNIX_SECONDS(CAST(time_stamp AS TIMESTAMP)) AS INTEGER) AS date_key,
        CAST(local_time AS STRING) AS local_time,
        ip AS ip_address,
        COALESCE(cp.product_id, -1) AS product_key,
        SAFE_CAST(cp.amount AS INT64) AS product_quantity,
        COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0) AS product_price,  
        COALESCE(NULLIF(TRIM(cp.currency), ''), 'Unknown') AS product_currency,
        COALESCE(SAFE_CAST(cp.amount AS INT64), 0) * COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0) AS line_total
    FROM ipJoin
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
FROM UniqueSalesOrder