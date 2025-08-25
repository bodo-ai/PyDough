SELECT
  o_orderkey AS order_key,
  RANK() OVER (ORDER BY CASE WHEN o_orderpriority COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, o_orderpriority COLLATE utf8mb4_bin) AS `rank`
FROM tpch.orders
