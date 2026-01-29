SELECT
  COUNT_IF(LOWER(food_type) = 'vegan') / CASE
    WHEN (
      COUNT(*) - COUNT_IF(LOWER(food_type) = 'vegan')
    ) <> 0
    THEN COUNT(*) - COUNT_IF(LOWER(food_type) = 'vegan')
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
