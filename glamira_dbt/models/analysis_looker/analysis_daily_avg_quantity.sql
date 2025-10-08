WITH fact_sales_order AS (
    SELECT
        date_key,
        product_quantity
    FROM {{ ref('fact_sales_order') }}
),

dim_date AS (
    SELECT
        date_key,
        date
    FROM {{ ref('dim_date') }}
),

daily_totals AS (
    SELECT
        dd.date,
        SUM(fso.product_quantity) AS total_quantity_per_day
    FROM fact_sales_order fso
    INNER JOIN dim_date dd
        ON fso.date_key = dd.date_key
    GROUP BY dd.date
),

overall_average AS (
    SELECT
        AVG(total_quantity_per_day) AS average_total_quantity_per_day
    FROM daily_totals
)

SELECT
    dt.date,
    dt.total_quantity_per_day,
    oa.average_total_quantity_per_day
FROM daily_totals dt
CROSS JOIN overall_average oa
ORDER BY dt.date