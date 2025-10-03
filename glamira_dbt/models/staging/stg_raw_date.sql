WITH UniqueDate AS (
    SELECT
        CAST(UNIX_SECONDS(CAST(time_stamp AS TIMESTAMP)) AS INTEGER) AS date_key,
        CAST(time_stamp AS DATE) AS date,
        EXTRACT(DAYOFWEEK FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_week,
        EXTRACT(DAY FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_month,
        EXTRACT(DAYOFYEAR FROM CAST(time_stamp AS TIMESTAMP)) AS day_of_year,
        EXTRACT(MONTH FROM CAST(time_stamp AS TIMESTAMP)) AS month,
        EXTRACT(YEAR FROM CAST(time_stamp AS TIMESTAMP)) AS year,
        ROW_NUMBER() OVER(PARTITION BY time_stamp ORDER BY time_stamp) AS rn
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
    UniqueDate
WHERE
    rn = 1
