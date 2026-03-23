SELECT
  name,
  rating
FROM postgres.main.restaurant
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
