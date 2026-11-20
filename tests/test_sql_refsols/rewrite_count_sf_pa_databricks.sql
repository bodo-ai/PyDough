SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.partsupp AS partsupp
JOIN tpch_s3_pq.tpch_sf1.part AS part
  ON part.p_brand = 'Brand#23' AND part.p_partkey = partsupp.ps_partkey
