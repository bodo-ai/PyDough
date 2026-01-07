SELECT
  CAST(COALESCE(SUM(CASE WHEN LOWER(food_type) = 'vegan' THEN 1 ELSE 0 END), 0) AS DOUBLE PRECISION) / NULLIF(SUM(CASE WHEN LOWER(food_type) <> 'vegan' THEN 1 ELSE 0 END), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
