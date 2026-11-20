SELECT
  COUNT(DISTINCT customer.c_custkey) AS n
FROM postgres.tpch.customer AS customer
JOIN mongo.tpch.supplier AS supplier
  ON customer.c_nationkey = supplier.s_nationkey
