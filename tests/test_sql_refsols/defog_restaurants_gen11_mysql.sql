SELECT
  SUM(rating > 4.5) / NULLIF(COUNT(*), 0) AS ratio
FROM restaurants.restaurant
