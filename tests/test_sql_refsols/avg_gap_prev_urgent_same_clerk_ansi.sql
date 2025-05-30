WITH _t0 AS (
  SELECT
    DATEDIFF(
      o_orderdate,
      LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate NULLS LAST),
      DAY
    ) AS delta
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(delta) AS avg_delta
FROM _t0
