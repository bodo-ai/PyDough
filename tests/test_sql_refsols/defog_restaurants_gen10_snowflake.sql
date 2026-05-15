SELECT
  name,
  rating
FROM restaurants.restaurant
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
