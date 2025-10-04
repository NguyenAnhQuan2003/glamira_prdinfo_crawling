SELECT 
    DISTINCT COALESCE(date_key, -1) AS date_key,
    date,
    day_of_month,
    day_of_week,
    day_of_year,
    month,
    year
FROM 
    {{ref('stg_raw_date')}}