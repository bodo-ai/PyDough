SELECT
  CAST(COALESCE(SUM(CASE WHEN LOWER(food_type) = 'vegan' THEN 1 ELSE 0 END), 0) AS DOUBLE PRECISION) / CASE
    WHEN (
      NOT SUM(CASE WHEN LOWER(food_type) <> 'vegan' THEN 1 ELSE 0 END) IS NULL
      AND SUM(CASE WHEN LOWER(food_type) <> 'vegan' THEN 1 ELSE 0 END) <> 0
    )
    THEN COALESCE(SUM(CASE WHEN LOWER(food_type) <> 'vegan' THEN 1 ELSE 0 END), 0)
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
