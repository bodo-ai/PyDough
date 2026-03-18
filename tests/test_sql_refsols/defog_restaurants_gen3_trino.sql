SELECT
  food_type,
  AVG(rating) AS avg_rating
FROM postgres.restaurant
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
