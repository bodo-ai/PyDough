SELECT
  name
FROM restaurants.restaurant
WHERE
  LOWER(food_type) = 'italian'
