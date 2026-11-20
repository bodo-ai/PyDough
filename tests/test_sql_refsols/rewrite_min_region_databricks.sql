SELECT
  MIN(n_regionkey) AS min_region
FROM tpch_s3_pq.tpch_sf1.nation
WHERE
  STARTSWITH(n_name, 'I')
