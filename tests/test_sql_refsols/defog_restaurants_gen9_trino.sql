SELECT
  name
FROM cassandra.defog.restaurant
ORDER BY
  rating DESC,
  1 DESC
LIMIT 3
