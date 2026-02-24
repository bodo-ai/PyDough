SELECT
  okey,
  odate
FROM defog.public.recent_orders_t10
WHERE
  odate < CAST('1995-06-01' AS DATE)
ORDER BY
  1
LIMIT 5
