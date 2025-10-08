WITH fact_sales_order AS (
    SELECT
        location_key,
        product_key,
        order_key,
        ip_address
    FROM {{ ref('fact_sales_order') }}
),
get_location AS (
    SELECT 
        ip.location_key AS location_key,
        ip.region AS region,
        ip.city AS city,
        dp.ip_address AS ip_address
    FROM fact_sales_order AS dp
    LEFT JOIN {{ref('dim_ip_location')}} AS ip
    ON dp.location_key = ip.location_key
)

SELECT
    location_key,
    region,
    city,
    ip_address
FROM get_location

