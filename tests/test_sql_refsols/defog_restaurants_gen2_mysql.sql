SELECT
  city_name,
  COUNT(*) AS total_count
FROM restaurants.location
GROUP BY
  1
