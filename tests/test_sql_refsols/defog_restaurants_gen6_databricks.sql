SELECT
  street_name
FROM defog.restaurants.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
