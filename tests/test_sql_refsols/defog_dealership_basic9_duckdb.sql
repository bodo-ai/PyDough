SELECT
  state,
  COUNT(*) AS total_signups
FROM main.customers
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 2
