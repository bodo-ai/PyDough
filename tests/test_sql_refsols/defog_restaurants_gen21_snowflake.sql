SELECT
  city_name
FROM main.restaurant
ORDER BY
  rating DESC NULLS LAST
LIMIT 1
