SELECT
  SUM(CASE WHEN rating > 4.5 THEN 1 ELSE 0 END) / COUNT(*) AS ratio
FROM main.restaurant
