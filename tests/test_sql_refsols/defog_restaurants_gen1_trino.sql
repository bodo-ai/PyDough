SELECT
  food_type,
  COUNT(*) AS restaurants
FROM postgres.restaurant
GROUP BY
  1
