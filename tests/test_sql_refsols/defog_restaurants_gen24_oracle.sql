SELECT
  name
FROM MAIN.RESTAURANT
WHERE
  LOWER(food_type) = 'italian'
