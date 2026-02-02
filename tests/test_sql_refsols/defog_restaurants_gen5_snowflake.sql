SELECT
  city_name,
  COUNT(*) AS num_restaurants
FROM restaurants.location
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
