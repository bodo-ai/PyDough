SELECT
  COALESCE(COUNT_IF(LOWER(food_type) = 'vegan'), 0) / CASE
    WHEN (
      COUNT_IF(LOWER(food_type) <> 'vegan') <> 0
      AND NOT COUNT_IF(LOWER(food_type) <> 'vegan') IS NULL
    )
    THEN COALESCE(COUNT_IF(LOWER(food_type) <> 'vegan'), 0)
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
