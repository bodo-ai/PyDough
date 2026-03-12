SELECT
  name,
  rating
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
