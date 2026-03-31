WITH _t0 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.ORDERS
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM _t0
