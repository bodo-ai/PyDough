SELECT
  name
FROM main.restaurant
WHERE
  LOWER(food_type) = 'italian'
