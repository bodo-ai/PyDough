SELECT
  name
FROM postgres.main.restaurant
WHERE
  LOWER(food_type) = 'italian'
