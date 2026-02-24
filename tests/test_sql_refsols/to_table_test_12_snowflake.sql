SELECT
  okey,
  total
FROM e2e_tests_db.public.order_summary_t12
WHERE
  total > 1000
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
