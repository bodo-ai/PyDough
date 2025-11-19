SELECT
  CAST(COALESCE(SUM(LOWER(food_type) = 'vegan'), 0) AS REAL) / CASE
    WHEN (
      NOT SUM(LOWER(food_type) <> 'vegan') IS NULL
      AND SUM(LOWER(food_type) <> 'vegan') <> 0
    )
    THEN COALESCE(SUM(LOWER(food_type) <> 'vegan'), 0)
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
