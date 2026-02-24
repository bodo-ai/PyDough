SELECT
  okey,
  total
FROM e2e_tests_db.public.expensive_orders_t6
ORDER BY
  2 DESC NULLS LAST
LIMIT 10
