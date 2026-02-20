SELECT
  COUNT_IF(rating > 4.0) / NULLIF(COUNT_IF(rating < 4.0), 0) AS ratio
FROM restaurants.restaurant
