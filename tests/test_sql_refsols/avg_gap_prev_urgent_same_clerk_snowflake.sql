WITH _t0 AS (
  SELECT
    DATEDIFF(
      DAY,
      CAST(LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate) AS DATETIME),
      CAST(o_orderdate AS DATETIME)
    ) AS delta
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(delta) AS avg_delta
FROM _t0
