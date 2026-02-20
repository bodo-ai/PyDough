WITH "_T0" AS (
  SELECT
    CAST(o_orderdate AS DATE) - CAST(LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate) AS DATE) AS DELTA
  FROM TPCH.ORDERS
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(DELTA) AS avg_delta
FROM "_T0"
