SELECT
  name COLLATE utf8mb4_bin AS name,
  rating
FROM restaurants.restaurant
ORDER BY
  2 DESC,
  1 DESC
