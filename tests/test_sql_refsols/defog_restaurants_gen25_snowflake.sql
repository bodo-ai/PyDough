SELECT
  name
FROM restaurants.restaurant
WHERE
  LOWER(city_name) = 'los angeles' AND rating > 4
