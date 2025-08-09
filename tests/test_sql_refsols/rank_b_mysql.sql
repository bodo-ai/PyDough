SELECT
  o_orderkey AS order_key,
  RANK() OVER (ORDER BY CASE WHEN o_orderpriority IS NULL THEN 1 ELSE 0 END, o_orderpriority) AS `rank`
FROM tpch.ORDERS
