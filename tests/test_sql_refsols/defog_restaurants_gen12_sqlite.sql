SELECT
  CAST(COALESCE(SUM(rating > 4.0), 0) AS REAL) / NULLIF(SUM(rating < 4.0), 0) AS ratio
FROM main.restaurant
