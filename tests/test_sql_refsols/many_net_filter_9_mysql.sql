SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey AND NATION.n_regionkey = 1
JOIN tpch.NATION AS NATION_2
  ON NATION.n_regionkey = NATION_2.n_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = SUPPLIER.s_suppkey
  AND CUSTOMER.c_nationkey = NATION_2.n_nationkey
