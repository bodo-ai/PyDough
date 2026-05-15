WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
  ORDER BY
    o_custkey,
    1
  LIMIT 10
)
SELECT
  DATE(o_orderdate, 'start of year') AS d1,
  DATE(o_orderdate, 'start of month') AS d2,
  DATETIME(o_orderdate, '-11 year', '9 month', '-7 day', '5 hour', '-3 minute', '1 second') AS d3,
  '2025-07-04 12:00:00' AS d4,
  '2025-07-04 12:58:00' AS d5,
  '2025-07-26 02:45:25' AS d6
FROM _t0
ORDER BY
  o_orderdate
