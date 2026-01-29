SELECT
  CAST(SUM(CASE WHEN LOWER(food_type) = 'vegan' THEN 1 ELSE 0 END) AS DOUBLE PRECISION) / CASE
    WHEN (
      COUNT(*) - SUM(CASE WHEN LOWER(food_type) = 'vegan' THEN 1 ELSE 0 END)
    ) <> 0
    THEN COUNT(*) - SUM(CASE WHEN LOWER(food_type) = 'vegan' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'san francisco'
