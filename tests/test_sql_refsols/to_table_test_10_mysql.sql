SELECT
  okey,
  odate
FROM recent_orders_t10
WHERE
  odate < CAST('1995-06-01' AS DATE)
ORDER BY
  CASE WHEN okey IS NULL THEN 1 ELSE 0 END,
  1
LIMIT 5
