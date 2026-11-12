SELECT
  COUNT(DISTINCT c_nationkey) AS n
FROM tpch_s3_pq.tpch_sf1.customer
WHERE
  c_acctbal < -975 AND c_mktsegment = 'AUTOMOBILE'
