SELECT
  oyear,
  COALESCE(SUM(n), 0) AS n,
  COALESCE(SUM(tot), 0) AS tot
FROM memory.default.monthly_orders_t26
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
