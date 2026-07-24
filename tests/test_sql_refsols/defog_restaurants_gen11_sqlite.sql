SELECT
  CAST(SUM(IIF(rating > 4.5, 1, 0)) AS REAL) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
