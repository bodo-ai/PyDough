SELECT
  name
FROM postgres.restaurant
WHERE
  LOWER(food_type) = 'italian'
