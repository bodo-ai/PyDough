SELECT
  city_name,
  COUNT(DISTINCT restaurant_id) AS total_count
FROM main.location
GROUP BY
  1
