SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM tpch_s3_pq.tpch_sf1.lineitem
WHERE
  l_quantity > 49
