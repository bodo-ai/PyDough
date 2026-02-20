SELECT
  street_name
FROM restaurants.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC NULLS LAST
LIMIT 1
