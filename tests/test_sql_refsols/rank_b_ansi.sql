SELECT
  key AS order_key,
  RANK() OVER (ORDER BY order_priority NULLS LAST) AS rank
FROM (
  SELECT
    o_orderkey AS key,
    o_orderpriority AS order_priority
  FROM tpch.ORDERS
) AS _t0
