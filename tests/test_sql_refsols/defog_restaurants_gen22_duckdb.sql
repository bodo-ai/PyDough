SELECT
  name,
  rating
FROM main.restaurant
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
