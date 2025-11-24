SELECT
  CAST(COALESCE(SUM(LOWER(food_type) = 'italian'), 0) AS REAL) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
