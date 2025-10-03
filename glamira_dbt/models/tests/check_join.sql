{{ config(materialized='table') }}

WITH BaseData AS (
    SELECT
        f.product_id,
        f.ip,
        ROW_NUMBER() OVER (PARTITION BY f.product_id, f.ip ORDER BY f.ip) AS rn
    FROM {{source('raw_dataset', 'raw_summary')}} AS f
),
JoinCheck AS (
    SELECT
        p.quantity AS quantity,
        p.product_price AS product_price,
        l.location_key AS location_key
    FROM BaseData bd
    INNER JOIN {{ref('dim_ip_location')}} AS l 
        ON bd.ip = l.ip
    INNER JOIN {{ref('dim_products')}} AS p 
        ON SAFE_CAST(bd.product_id AS INTEGER) = p.product_key
    WHERE bd.rn = 1
    GROUP BY bd.product_id, p.quantity, p.product_price, l.location_key, bd.ip
)
SELECT * FROM JoinCheck