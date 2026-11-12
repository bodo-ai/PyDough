SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch.nation AS nation_2
  ON nation.n_regionkey = nation_2.n_regionkey AND nation_2.n_nationkey = 6
JOIN tpch.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = nation_2.n_nationkey
