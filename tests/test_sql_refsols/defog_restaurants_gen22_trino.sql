SELECT
  name,
  rating
FROM postgres.restaurant
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
