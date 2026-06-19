SELECT
  COALESCE(SUM(rating > 4.0), 0) / NULLIF(SUM(rating < 4.0), 0) AS ratio
FROM restaurants.restaurant
