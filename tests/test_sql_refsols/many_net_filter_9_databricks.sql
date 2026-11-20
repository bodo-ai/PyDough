SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.supplier AS supplier
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey AND nation.n_regionkey = 1
JOIN tpch_s3_pq.tpch_sf1.nation AS nation_2
  ON nation.n_regionkey = nation_2.n_regionkey
JOIN tpch_s3_pq.tpch_sf1.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = nation_2.n_nationkey
