SELECT
  part.p_partkey AS key,
  region.r_name AS region,
  DENSE_RANK() OVER (
    PARTITION BY nation.n_regionkey
    ORDER BY part.p_size DESC NULLS FIRST, part.p_container DESC NULLS FIRST, part.p_type DESC NULLS FIRST
  ) AS rank
FROM tpch_s3_pq.tpch_sf1.region AS region
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch_s3_pq.tpch_sf1.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch_s3_pq.tpch_sf1.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch_s3_pq.tpch_sf1.part AS part
  ON part.p_partkey = partsupp.ps_partkey
ORDER BY
  1,
  2
LIMIT 15
