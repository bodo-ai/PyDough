SELECT
  city_name,
  COUNT(*) AS num_restaurants
FROM defog.restaurants.location
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
