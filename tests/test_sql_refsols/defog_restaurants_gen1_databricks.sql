SELECT
  food_type,
  COUNT(*) AS restaurants
FROM defog.restaurants.restaurant
GROUP BY
  1
