SELECT
  c_nationkey AS key,
  c_nationkey AS key_again,
  COUNT(*) AS cnt
FROM tpch_s3_pq.tpch_sf1.customer
GROUP BY
  1
ORDER BY
  1
