SELECT
  oyear,
  COALESCE(n, 0) AS n,
  COALESCE(tot, 0) AS tot
FROM e2e_tests_db.public.yearly_orders_t27
ORDER BY
  1 NULLS FIRST
