SELECT
  CAST(SUM(IIF(LOWER(food_type) = 'italian', 1, 0)) AS REAL) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
