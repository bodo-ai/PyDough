SELECT
  okey,
  total
FROM order_summary_t12
WHERE
  total > 1000
ORDER BY
  2 DESC
LIMIT 5
