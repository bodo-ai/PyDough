SELECT
  CAST(SUM(rating > 4.5) AS REAL) / COUNT(*) AS ratio
FROM main.restaurant
