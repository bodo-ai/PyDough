WITH _t1 AS (
  SELECT
    o_orderdate AS order_date
  FROM tpch.orders
  ORDER BY
    o_custkey,
    order_date
  LIMIT 10
)
SELECT
  DATE(order_date, 'start of year') AS d1,
  DATE(order_date, 'start of month') AS d2,
  DATETIME(order_date, '-11 year', '9 month', '-7 day', '5 hour', '-3 minute', '1 second') AS d3,
  STRFTIME('%Y-%m-%d %H:00:00', DATETIME('2025-07-04 12:58:45')) AS d4,
  STRFTIME('%Y-%m-%d %H:%M:00', DATETIME('2025-07-04 12:58:45')) AS d5,
  DATETIME('2025-07-14 12:58:45', '1000000 second') AS d6
FROM _t1
ORDER BY
  order_date
