SELECT
  CAST(SUM(IIF(rating > 4.5, 1, 0)) AS REAL) / COUNT(*) AS ratio
FROM main.restaurant
