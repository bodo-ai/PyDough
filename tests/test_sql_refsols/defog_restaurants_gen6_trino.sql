SELECT
  street_name
FROM postgres.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
