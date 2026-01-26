SELECT
  SUM(IFF(rating > 4.5, 1, 0)) / COUNT(*) AS ratio
FROM main.restaurant
