WITH _t0 AS (
  SELECT
    CAST(o_orderdate AS DATE) - CAST(LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate) AS DATE) AS delta
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(CAST(delta AS DECIMAL)) AS avg_delta
FROM _t0
