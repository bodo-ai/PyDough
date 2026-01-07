SELECT
  city_name COLLATE utf8mb4_bin AS city_name,
  COUNT(*) AS num_restaurants
FROM main.restaurant
WHERE
  LOWER(food_type) = 'italian'
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
