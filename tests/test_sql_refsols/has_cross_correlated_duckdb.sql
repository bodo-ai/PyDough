SELECT
  COUNT(DISTINCT customer.c_custkey) AS n
FROM main.customer AS customer
JOIN main.supplier AS supplier
  ON customer.c_nationkey = supplier.s_nationkey
