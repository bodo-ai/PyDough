SELECT
  COALESCE(SUM(LOWER(food_type) = 'vegan'), 0) / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM restaurants.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
