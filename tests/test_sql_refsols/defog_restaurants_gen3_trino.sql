SELECT
  food_type,
  AVG(CAST(rating AS DOUBLE)) AS avg_rating
FROM cassandra.defog.restaurant
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
