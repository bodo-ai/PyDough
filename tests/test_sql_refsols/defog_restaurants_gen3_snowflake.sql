SELECT
  food_type,
  AVG(rating) AS avg_rating
FROM restaurants.restaurant
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
