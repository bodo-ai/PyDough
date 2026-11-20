SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_orderpriority = '1-URGENT'
