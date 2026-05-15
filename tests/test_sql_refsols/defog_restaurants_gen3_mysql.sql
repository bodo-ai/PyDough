SELECT
  food_type COLLATE utf8mb4_bin AS food_type,
  AVG(rating) AS avg_rating
FROM restaurants.restaurant
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
