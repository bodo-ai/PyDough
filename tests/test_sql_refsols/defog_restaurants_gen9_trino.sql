SELECT
  name
FROM postgres.restaurant
ORDER BY
  rating DESC,
  1 DESC
LIMIT 3
