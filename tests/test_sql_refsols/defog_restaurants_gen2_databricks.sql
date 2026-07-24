SELECT
  city_name,
  COUNT(*) AS total_count
FROM defog.restaurants.location
GROUP BY
  1
