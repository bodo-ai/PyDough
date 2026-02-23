SELECT
  name
FROM restaurants.restaurant
WHERE
  LOWER(city_name) = 'new york' OR LOWER(food_type) = 'italian'
