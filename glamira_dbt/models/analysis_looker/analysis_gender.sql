WITH checkSuccess AS (
    SELECT
        product_key
    FROM
        {{source('fact_glamira', 'fact_sales_order')}}
),
joinProduct AS (
    SELECT 
        cs.product_key AS product_key,
        dp.product_gender AS product_gender
    FROM checkSuccess cs
    INNER JOIN {{source('dim_glamira', 'dim_products')}} dp
    ON dp.product_key = cs.product_key
    WHERE dp.product_gender != "Unknown"
        AND dp.product_gender != "false"
)
SELECT 
    product_key,
    product_gender
FROM joinProduct