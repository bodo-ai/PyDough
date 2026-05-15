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
  DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)) AS d1,
  DATE_TRUNC('MONTH', CAST(o_orderdate AS TIMESTAMP)) AS d2,
  DATE_ADD(
    DATE_SUB(
      DATE_ADD(
        DATE_SUB(DATE_ADD(DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 11, YEAR), 9, 'MONTH'), 7, DAY),
        5,
        'HOUR'
      ),
      3,
      MINUTE
    ),
    1,
    'SECOND'
  ) AS d3,
  CAST('2025-07-04 12:00:00' AS TIMESTAMP) AS d4,
  CAST('2025-07-04 12:58:00' AS TIMESTAMP) AS d5,
  CAST('2025-07-26 02:45:25' AS TIMESTAMP) AS d6
FROM _t0
ORDER BY
  o_orderdate
