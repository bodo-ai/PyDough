SELECT
  name
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles' AND rating > 4
