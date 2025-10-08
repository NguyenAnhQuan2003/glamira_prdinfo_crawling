WITH fact_sales_order AS (
    SELECT
        line_total_usd
    FROM {{ ref('fact_sales_order') }}
)

SELECT
    SUM(line_total_usd) AS total_revenue
FROM fact_sales_order