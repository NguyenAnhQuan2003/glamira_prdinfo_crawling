SELECT
  FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(ip), ''), COALESCE(TRIM(city), ''), COALESCE(TRIM(region), ''))) AS location_key,
  COALESCE(TRIM(city), 'Unknown') AS city,
  COALESCE(TRIM(region), 'Unknown') AS region,
  COALESCE(TRIM(country_short), 'Unknown') AS country_short,
  COALESCE(TRIM(country_long), 'Unknown') AS country_long,
  COALESCE(TRIM(ip), 'Unknown') AS ip
FROM {{ source('raw_dataset', 'raw_ip_location') }}
WHERE TRIM(ip) IS NOT NULL AND TRIM(ip) != '' AND TRIM(ip) != '-'
  AND TRIM(city) IS NOT NULL AND TRIM(city) != '' AND TRIM(city) != '-'
  AND TRIM(region) IS NOT NULL AND TRIM(region) != '' AND TRIM(region) != '-'
  AND TRIM(country_short) IS NOT NULL AND TRIM(country_short) != '' AND TRIM(country_short) != '-'
  AND TRIM(country_long) IS NOT NULL AND TRIM(country_long) != '' AND TRIM(country_long) != '-'