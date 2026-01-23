WITH _t1 AS (
  SELECT
    o_orderkey,
    o_orderpriority,
    o_totalprice
  FROM tpch.orders
  WHERE
    YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1992
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY o_totalprice DESC) = 1
)
SELECT
  o_orderpriority AS order_priority,
  o_orderkey AS order_key,
  o_totalprice AS order_total_price
FROM _t1
ORDER BY
  1 NULLS FIRST
