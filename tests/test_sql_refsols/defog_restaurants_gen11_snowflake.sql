SELECT
  COUNT_IF(rating > 4.5) / COUNT(*) AS ratio
FROM main.restaurant
