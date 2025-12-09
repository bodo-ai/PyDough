SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN tpch.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = supplier.s_nationkey
WHERE
  supplier.s_nationkey = 1
