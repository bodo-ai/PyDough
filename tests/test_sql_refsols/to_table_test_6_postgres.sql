SELECT
  okey,
  total
FROM expensive_orders_t6
ORDER BY
  2 DESC NULLS LAST
LIMIT 10
