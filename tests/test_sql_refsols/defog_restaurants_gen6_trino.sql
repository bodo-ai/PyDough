SELECT
  street_name
FROM postgres.main.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
