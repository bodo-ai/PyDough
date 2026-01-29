SELECT
  name,
  rating
FROM restaurants.restaurant
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
