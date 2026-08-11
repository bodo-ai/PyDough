SELECT
  CAST(SUM(IIF(LOWER(food_type) = 'vegan', 1, 0)) AS REAL) / NULLIF(SUM(IIF(LOWER(food_type) <> 'vegan', 1, 0)), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
  AND (
    LOWER(food_type) <> 'vegan' OR LOWER(food_type) = 'vegan'
  )
