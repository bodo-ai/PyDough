SELECT
  CAST(COALESCE(SUM(CASE WHEN LOWER(food_type) = 'italian' THEN 1 ELSE 0 END), 0) AS DOUBLE PRECISION) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
