SELECT
  okey,
  odate
FROM recent_orders_t10
WHERE
  odate < CAST('1995-06-01' AS DATE)
ORDER BY
  2
LIMIT 5
