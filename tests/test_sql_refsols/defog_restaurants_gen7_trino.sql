SELECT
  name
FROM postgres.main.restaurant
WHERE
  LOWER(city_name) = 'new york' OR LOWER(food_type) = 'italian'
