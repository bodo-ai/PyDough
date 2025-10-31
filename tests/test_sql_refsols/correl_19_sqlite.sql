SELECT
  MAX(supplier.s_name) AS supplier_name,
  COUNT(*) AS n_super_cust
FROM tpch.supplier AS supplier
JOIN tpch.customer AS customer
  ON customer.c_acctbal > supplier.s_acctbal
  AND customer.c_nationkey = supplier.s_nationkey
WHERE
  supplier.s_nationkey = supplier.s_nationkey
GROUP BY
  supplier.s_suppkey
ORDER BY
  2 DESC
LIMIT 5
