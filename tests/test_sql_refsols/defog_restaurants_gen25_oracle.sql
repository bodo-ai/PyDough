SELECT
  name
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'los angeles' AND rating > 4
