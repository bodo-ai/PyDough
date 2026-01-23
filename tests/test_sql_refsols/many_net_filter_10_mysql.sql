SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN tpch.NATION AS NATION_2
  ON NATION.n_regionkey = NATION_2.n_regionkey AND NATION_2.n_regionkey = 2
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = SUPPLIER.s_suppkey
  AND CUSTOMER.c_nationkey = NATION_2.n_nationkey
