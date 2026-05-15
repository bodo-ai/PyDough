WITH _t0 AS (
  SELECT
    DATEDIFF(
      o_orderdate,
      LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate)
    ) AS delta
  FROM tpch.ORDERS
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(delta) AS avg_delta
FROM _t0
