SELECT
  name COLLATE utf8mb4_bin AS name
FROM restaurants.restaurant
ORDER BY
  rating DESC,
  1 DESC
LIMIT 3
