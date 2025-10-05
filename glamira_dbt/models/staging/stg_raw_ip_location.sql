SELECT
  DISTINCT FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(country_long), ''), COALESCE(TRIM(city), ''), COALESCE(TRIM(region), ''))) AS location_key,
  COALESCE(NULLIF(TRIM(city), ''), 'Unknown') AS city,
  COALESCE(NULLIF(TRIM(region), ''), 'Unknown') AS region,
  COALESCE(NULLIF(TRIM(country_short), ''), 'Unknown') AS country_short,
  COALESCE(NULLIF(TRIM(country_long), ''), 'Unknown') AS country_name,
FROM {{ source('raw_dataset', 'raw_ip_location') }}
WHERE TRIM(city) IS NOT NULL AND TRIM(city) != '' AND TRIM(city) != '-'
  AND TRIM(region) IS NOT NULL AND TRIM(region) != '' AND TRIM(region) != '-'
  AND TRIM(country_short) IS NOT NULL AND TRIM(country_short) != '' AND TRIM(country_short) != '-'
  AND TRIM(country_long) IS NOT NULL AND TRIM(country_long) != '' AND TRIM(country_long) != '-'