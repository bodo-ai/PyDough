SELECT
  CAST(SUM(rating > 4.5) AS DOUBLE) / NULLIF(COUNT(*), 0) AS ratio
FROM main.restaurant
