SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey AND NATION.n_regionkey < 3
JOIN tpch.NATION AS NATION_2
  ON NATION.n_regionkey = NATION_2.n_regionkey AND NATION_2.n_regionkey > 0
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = SUPPLIER.s_suppkey
  AND CUSTOMER.c_nationkey = NATION_2.n_nationkey
  AND NOT CUSTOMER.c_nationkey IN (1, 4, 7, 10, 13, 16, 19, 22)
WHERE
  NOT SUPPLIER.s_nationkey IN (0, 3, 6, 9, 12, 15, 18, 21, 24)
