WITH Step1_RawFilter AS (
    SELECT
        ip,
        order_id,
        product_id,
        user_id_db,
        time_stamp,
        local_time
    FROM {{source('raw_dataset', 'raw_summary')}}
),

Step2_IpJoin AS (
    SELECT
        rf.*,
        l.location_key
    FROM Step1_RawFilter rf
    LEFT JOIN {{ref('dim_ip_location')}} AS l 
        ON rf.ip = l.ip
),

Step3_ProductJoin AS (
    SELECT
        ij.*,
        p.quantity,
        p.product_price
    FROM Step2_IpJoin ij
    LEFT JOIN {{ref('dim_products')}} AS p 
        ON SAFE_CAST(ij.product_id AS INTEGER) = p.product_key
),

UniqueSalesOrder AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(SAFE_CAST(ip AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(order_id AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(product_id AS STRING)), ''))) AS sales_hash_key,
        SAFE_CAST(product_id AS INTEGER) AS product_key,
        COALESCE(TRIM('user_id_db', 'Unknown')) AS user_key,
        location_key,
        CAST(UNIX_SECONDS(CAST(time_stamp AS TIMESTAMP)) AS INTEGER) AS date_key,
        COALESCE(TRIM('order_id', 'Unknown')) AS order_key,
        CAST(local_time AS STRING) AS local_time,
        ip AS ip_address,
        quantity,
        product_price AS price,
        ROW_NUMBER() OVER (PARTITION BY product_id, ip ORDER BY time_stamp) AS rn
    FROM Step3_ProductJoin
    WHERE location_key IS NOT NULL AND quantity IS NOT NULL
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
    quantity,
    price
FROM UniqueSalesOrder
WHERE rn = 1