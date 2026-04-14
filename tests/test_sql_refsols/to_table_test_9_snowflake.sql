SELECT
  okey,
  total,
  ROW_NUMBER() OVER (ORDER BY total DESC) AS rank
FROM e2e_tests_db.public.order_summary_t9
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
