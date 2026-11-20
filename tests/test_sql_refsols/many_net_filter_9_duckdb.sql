SELECT
  COUNT(*) AS n
FROM main.supplier AS supplier
JOIN main.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey AND nation.n_regionkey = 1
JOIN main.nation AS nation_2
  ON nation.n_regionkey = nation_2.n_regionkey
JOIN main.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = nation_2.n_nationkey
