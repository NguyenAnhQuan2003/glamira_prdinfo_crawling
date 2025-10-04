WITH FactSalesOrder AS (
    SELECT
        sso.sales_hash_key,
        sso.product_key,
        sso.user_key,
        sso.location_key,
        sso.date_key,
        sso.order_key,
        sso.local_time,
        sso.ip_address,
        sso.product_quantity,
        sso.product_price,
        sso.product_currency,
        sso.line_total
    FROM {{ ref('stg_fact_sales_order') }} sso
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
FROM FactSalesOrder