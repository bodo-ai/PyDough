SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey AND nation.n_regionkey < 3
JOIN tpch.nation AS nation_2
  ON nation.n_regionkey = nation_2.n_regionkey AND nation_2.n_regionkey > 0
JOIN tpch.customer AS customer
  ON NOT customer.c_nationkey IN (1, 4, 7, 10, 13, 16, 19, 22)
  AND customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = nation_2.n_nationkey
WHERE
  NOT supplier.s_nationkey IN (0, 3, 6, 9, 12, 15, 18, 21, 24)
