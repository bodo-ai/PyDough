SELECT
  okey,
  total
FROM defog.public.expensive_orders_t6
ORDER BY
  2 DESC NULLS LAST
LIMIT 10
