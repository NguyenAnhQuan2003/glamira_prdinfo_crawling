WITH daily_success_order AS (
    SELECT
        order_key,
        date_key
    FROM
        {{ref('fact_sales_order')}}
),

join_dim_date AS (
    SELECT
        dd.date,
        COUNT(DISTINCT ds.order_key) AS total_orders_per_day  
    FROM daily_success_order AS ds
    INNER JOIN {{ref('dim_date')}} AS dd
        ON ds.date_key = dd.date_key
    GROUP BY dd.date
),

daily_avg_order AS (
    SELECT
        AVG(total_orders_per_day) AS avg_total_orders_per_day  
    FROM join_dim_date
)

SELECT
    avg_total_orders_per_day
FROM daily_avg_order