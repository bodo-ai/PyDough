WITH _t0 AS (
  SELECT
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', o_orderdate) AS TIMESTAMP)
    ) AS delta
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(delta) AS avg_delta
FROM _t0
