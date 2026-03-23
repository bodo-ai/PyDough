SELECT
  food_type,
  COUNT(*) AS restaurants
FROM postgres.main.restaurant
GROUP BY
  1
