SELECT
  city_name COLLATE utf8mb4_bin AS city_name,
  COUNT(*) AS num_restaurants
FROM restaurants.location
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
