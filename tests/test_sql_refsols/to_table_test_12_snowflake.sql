SELECT
  okey,
  total
FROM defog.public.order_summary_t12
WHERE
  total > 1000
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
