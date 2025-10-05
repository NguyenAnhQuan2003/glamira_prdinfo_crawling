WITH checkSuccess AS (
    SELECT
        product_key,
        line_total,
        product_price
    FROM {{source('fact_glamira', 'fact_sales_order')}}
),
joinProduct AS (
    SELECT 
        cs.product_key AS product_key,
        cs.line_total AS line_total,
        dp.product_name AS product_name,
        cs.product_price,
        dp.product_gender AS product_gender
    FROM checkSuccess cs
    INNER JOIN {{source('dim_glamira', 'dim_products')}} dp
    ON dp.product_key = cs.product_key
    WHERE dp.product_name != "Unknown"
        AND dp.product_gender != "false"
),
RankedProducts AS (
    SELECT 
        product_key,
        product_name,
        product_price,
        product_gender,
        line_total,
        ROW_NUMBER() OVER (PARTITION BY product_key ORDER BY line_total DESC) AS rn
    FROM joinProduct
)
SELECT 
    product_key,
    product_name,
    product_price,
    product_gender,
    line_total,
FROM RankedProducts
WHERE rn = 1
ORDER BY line_total DESC
LIMIT 10