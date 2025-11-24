SELECT
  CAST(COALESCE(SUM(LOWER(food_type) = 'vegan'), 0) AS REAL) / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
