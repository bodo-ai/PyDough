SELECT
  state,
  COUNT(*) AS total_signups
FROM postgres.customers
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 2
