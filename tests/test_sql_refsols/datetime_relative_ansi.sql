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
  DATE_TRUNC('YEAR', CAST(order_date AS TIMESTAMP)) AS d1,
  DATE_TRUNC('MONTH', CAST(order_date AS TIMESTAMP)) AS d2,
  DATE_ADD(
    DATE_ADD(
      DATE_ADD(
        DATE_ADD(
          DATE_ADD(DATE_ADD(CAST(order_date AS TIMESTAMP), -11, 'YEAR'), 9, 'MONTH'),
          -7,
          'DAY'
        ),
        5,
        'HOUR'
      ),
      -3,
      'MINUTE'
    ),
    1,
    'SECOND'
  ) AS d3,
  DATE_TRUNC('HOUR', CAST(CAST('2025-07-04 12:58:45' AS TIMESTAMP) AS TIMESTAMP)) AS d4,
  DATE_TRUNC('MINUTE', CAST(CAST('2025-07-04 12:58:45' AS TIMESTAMP) AS TIMESTAMP)) AS d5,
  CAST('2025-07-26 02:45:25' AS TIMESTAMP) AS d6
FROM _t1
ORDER BY
  order_date
