WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
  ORDER BY
    o_custkey NULLS FIRST,
    1 NULLS FIRST
  LIMIT 10
)
SELECT
  DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)) AS d1,
  DATE_TRUNC('MONTH', CAST(o_orderdate AS TIMESTAMP)) AS d2,
  DATE_ADD(
    'SECOND',
    1,
    DATE_ADD(
      'MINUTE',
      -3,
      DATE_ADD(
        'HOUR',
        5,
        DATE_ADD(
          'DAY',
          -7,
          DATE_ADD('MONTH', 9, DATE_ADD('YEAR', -11, CAST(o_orderdate AS TIMESTAMP)))
        )
      )
    )
  ) AS d3,
  CAST('2025-07-04 12:00:00' AS TIMESTAMP) AS d4,
  CAST('2025-07-04 12:58:00' AS TIMESTAMP) AS d5,
  CAST('2025-07-26 02:45:25' AS TIMESTAMP) AS d6
FROM _t0
ORDER BY
  o_orderdate NULLS FIRST
