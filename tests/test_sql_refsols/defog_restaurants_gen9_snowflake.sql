SELECT
  name
FROM restaurants.restaurant
ORDER BY
  rating DESC NULLS LAST,
  1 DESC NULLS LAST
LIMIT 3
