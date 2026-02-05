SELECT
  name
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'new york' OR LOWER(food_type) = 'italian'
