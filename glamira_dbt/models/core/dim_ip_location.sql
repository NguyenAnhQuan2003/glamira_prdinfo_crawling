SELECT
  location_key,
  COALESCE(TRIM(city), 'Unknown') AS city,
  COALESCE(TRIM(region), 'Unknown') AS region,
  COALESCE(TRIM(country_short), 'Unknown') AS country_short,
  COALESCE(TRIM(country_long), 'Unknown') AS country_long,
  COALESCE(TRIM(ip), 'Unknown') AS ip,
FROM {{ ref('stg_raw_ip_location') }}