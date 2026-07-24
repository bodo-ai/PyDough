SELECT
  COUNT_IF(LOWER(food_type) = 'vegan') / NULLIF(COUNT_IF(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM defog.restaurants.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
