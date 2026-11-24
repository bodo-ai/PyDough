SELECT
  region.r_regionkey AS rk,
  nation.n_nationkey AS nk,
  orders.o_orderkey AS oid,
  COUNT(orders.o_orderkey) OVER (PARTITION BY nation.n_regionkey) AS n,
  SUM(orders.o_totalprice) OVER (PARTITION BY nation.n_regionkey) AS total
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_nationkey <= 2 AND nation.n_regionkey = region.r_regionkey
JOIN tpch.orders AS orders
  ON orders.o_custkey <= 2
WHERE
  region.r_regionkey = 1
