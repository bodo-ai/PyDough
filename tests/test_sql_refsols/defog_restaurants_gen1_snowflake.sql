SELECT
  food_type,
  COUNT(*) AS restaurants
FROM restaurants.restaurant
GROUP BY
  1
