SELECT
  SUM(LOWER(food_type) = 'italian') / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
