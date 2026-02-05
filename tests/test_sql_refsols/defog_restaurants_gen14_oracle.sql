SELECT
  NVL(SUM(LOWER(food_type) = 'vegan'), 0) / NULLIF(SUM(LOWER(food_type) <> 'vegan'), 0) AS ratio
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'san francisco'
