SELECT
  COUNT(DISTINCT customer.c_custkey) AS n
FROM tpch.customer AS customer
JOIN tpch.supplier AS supplier
  ON customer.c_nationkey = supplier.s_nationkey
