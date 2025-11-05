SELECT
  CAST(COALESCE(SUM(LOWER(food_type) = 'italian'), 0) AS REAL) / CASE WHEN COUNT(*) <> 0 THEN COUNT(*) ELSE NULL END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
