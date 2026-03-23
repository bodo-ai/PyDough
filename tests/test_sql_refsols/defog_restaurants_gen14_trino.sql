SELECT
  CAST(COUNT_IF(LOWER(food_type) = 'vegan') AS DOUBLE) / NULLIF(COUNT_IF(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM postgres.main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
