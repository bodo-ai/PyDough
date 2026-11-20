SELECT
  COUNT(*) AS n
FROM main.supplier AS supplier
JOIN main.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = supplier.s_nationkey
WHERE
  supplier.s_nationkey = 1
