SELECT
  SUM(LOWER(food_type) = 'vegan') / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM main.restaurant
WHERE
  (
    LOWER(city_name) = 'san francisco' OR LOWER(food_type) = 'vegan'
  )
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
