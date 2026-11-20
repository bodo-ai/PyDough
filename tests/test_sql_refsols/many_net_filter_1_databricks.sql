SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.supplier AS supplier
JOIN tpch_s3_pq.tpch_sf1.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = supplier.s_nationkey
WHERE
  supplier.s_nationkey = 1
