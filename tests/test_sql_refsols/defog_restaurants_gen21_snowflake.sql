SELECT
  city_name
FROM restaurants.restaurant
ORDER BY
  rating DESC NULLS LAST
LIMIT 1
