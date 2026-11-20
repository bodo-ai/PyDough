SELECT
  o_custkey AS key,
  COALESCE(SUM(o_totalprice), 0) AS total
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_custkey <= 3
GROUP BY
  1
