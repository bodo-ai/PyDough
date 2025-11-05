SELECT
  city_name,
  COUNT(*) AS total_count
FROM main.location
GROUP BY
  1
