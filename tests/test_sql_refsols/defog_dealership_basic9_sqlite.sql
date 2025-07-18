SELECT
  state,
  COUNT(*) AS total_signups
FROM main.customers
GROUP BY
  state
ORDER BY
  total_signups DESC
LIMIT 2
