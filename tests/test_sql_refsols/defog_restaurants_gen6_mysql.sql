SELECT
  street_name
FROM main.location
GROUP BY
  1
ORDER BY
  COUNT(DISTINCT restaurant_id) DESC
LIMIT 1
