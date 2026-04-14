SELECT
  SUM(LOWER(food_type) = 'vegan') / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'san francisco'
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
