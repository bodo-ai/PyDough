SELECT
  city_name,
  COUNT(*) AS total_count
FROM MAIN.LOCATION
GROUP BY
  city_name
