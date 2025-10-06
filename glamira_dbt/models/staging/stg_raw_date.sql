WITH stg_raw_date_get_raw_summary AS (
    SELECT
        DISTINCT CAST(FORMAT_DATE('%Y%m%d', CAST(time_stamp AS DATE)) AS INT64) AS date_key,
        EXTRACT(DAY FROM CAST(time_stamp AS DATE)) AS date,
        EXTRACT(DAYOFWEEK FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_week,
        EXTRACT(DAY FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_month,
        EXTRACT(DAYOFYEAR FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_year,
        EXTRACT(MONTH FROM CAST(time_stamp AS TIMESTAMP)) AS month,
        EXTRACT(YEAR FROM CAST(time_stamp AS TIMESTAMP)) AS year,
    FROM
        {{source('raw_dataset', 'raw_summary')}}
    WHERE
        time_stamp IS NOT NULL
        AND TRIM(SAFE_CAST(time_stamp AS STRING)) != ''
        AND TRIM(SAFE_CAST(time_stamp AS STRING)) != '-'
)
SELECT 
    date_key,
    date,
    day_of_month,
    day_of_week,
    day_of_year,
    month,
    year
FROM 
    stg_raw_date_get_raw_summary
