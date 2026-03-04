SELECT
  street_name
FROM main.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
