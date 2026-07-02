SELECT
  name
FROM defog.restaurants.restaurant
WHERE
  LOWER(food_type) = 'italian'
