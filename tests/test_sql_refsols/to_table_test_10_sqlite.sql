SELECT
  okey,
  odate
FROM recent_orders_t10
WHERE
  odate < '1995-06-01'
ORDER BY
  2
LIMIT 5
