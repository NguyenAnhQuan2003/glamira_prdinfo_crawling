WITH checkSuccess AS (
    SELECT
        product_key,
        line_total_usd,
        product_price_usd
    FROM {{ref('fact_sales_order')}}
),
joinProduct AS (
    SELECT 
        cs.product_key AS product_key,
        cs.line_total_usd AS line_total_usd,
        dp.product_name AS product_name,
        cs.product_price_usd,
        dp.product_gender AS product_gender
    FROM checkSuccess cs
    INNER JOIN {{ref('dim_products')}} dp
    ON dp.product_key = cs.product_key
    WHERE dp.product_name != "Unknown"
        AND dp.product_gender != "false"
),
RankedProducts AS (
    SELECT 
        product_key,
        product_name,
        product_price_usd,
        product_gender,
        line_total_usd,
        ROW_NUMBER() OVER (PARTITION BY product_key ORDER BY line_total_usd DESC) AS rn
    FROM joinProduct
)
SELECT 
    product_key,
    product_name,
    product_price_usd,
    product_gender,
    line_total_usd,
FROM RankedProducts
WHERE rn = 1
ORDER BY line_total_usd DESC
LIMIT 10