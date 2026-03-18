SELECT
  name
FROM postgres.restaurant
WHERE
  LOWER(city_name) = 'los angeles' AND rating > 4
