SELECT
  COUNT(DISTINCT customer.c_custkey) AS n
FROM tpch_s3_pq.tpch_sf1.customer AS customer
JOIN tpch_s3_pq.tpch_sf1.supplier AS supplier
  ON customer.c_nationkey = supplier.s_nationkey
