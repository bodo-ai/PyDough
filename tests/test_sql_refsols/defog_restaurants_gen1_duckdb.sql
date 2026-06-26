SELECT
  food_type,
  COUNT(*) AS restaurants
FROM main.restaurant
GROUP BY
  1
