WITH _t0 AS (
  SELECT
    o_orderkey AS key,
    o_orderpriority AS order_priority,
    o_totalprice AS total_price
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM o_orderdate) = 1992
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY o_totalprice DESC NULLS FIRST) = 1
)
SELECT
  order_priority,
  key AS order_key,
  total_price AS order_total_price
FROM _t0
ORDER BY
  order_priority
