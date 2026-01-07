SELECT
  name COLLATE utf8mb4_bin AS name,
  rating
FROM main.restaurant
ORDER BY
  2 DESC,
  1 DESC
