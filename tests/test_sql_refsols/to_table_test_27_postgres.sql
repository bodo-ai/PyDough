SELECT
  oyear,
  COALESCE(n, 0) AS n,
  COALESCE(tot, 0) AS tot
FROM yearly_orders_t27
ORDER BY
  1 NULLS FIRST
