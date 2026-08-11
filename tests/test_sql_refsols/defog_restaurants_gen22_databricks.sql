SELECT
  name,
  rating
FROM defog.restaurants.restaurant
WHERE
  LOWER(city_name) = 'new york' AND rating > 4
