SELECT
  CAST(SUM(LOWER(food_type) = 'vegan') AS REAL) / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
