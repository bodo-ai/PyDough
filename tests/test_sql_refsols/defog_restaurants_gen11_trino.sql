SELECT
  CAST(COUNT_IF(rating > 4.5) AS DOUBLE) / NULLIF(COUNT(*), 0) AS ratio
FROM postgres.restaurant
