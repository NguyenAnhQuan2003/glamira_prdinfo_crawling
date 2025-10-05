SELECT
    DISTINCT location_key,
    city,
    region,
    country_short,
    country_name
FROM {{ ref('stg_raw_ip_location') }}