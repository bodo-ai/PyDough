SELECT
  COUNT_IF(rating > 4.5) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
