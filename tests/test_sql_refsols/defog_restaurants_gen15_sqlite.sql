SELECT
  CAST(SUM(LOWER(food_type) = 'italian') AS REAL) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
