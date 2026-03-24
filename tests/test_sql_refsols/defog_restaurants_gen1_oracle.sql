SELECT
  food_type,
  COUNT(*) AS restaurants
FROM MAIN.RESTAURANT
GROUP BY
  food_type
