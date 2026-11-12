SELECT
  REPLACE(c_custkey, '1', 'one') AS clean_key
FROM tpch_s3_pq.tpch_sf1.customer
WHERE
  c_custkey <= 5
ORDER BY
  c_custkey
