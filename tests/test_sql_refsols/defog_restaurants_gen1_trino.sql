SELECT
  food_type,
  COUNT(*) AS restaurants
FROM cassandra.defog.restaurant
GROUP BY
  1
