SELECT
  o_orderkey AS key
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_totalprice < 1000.0
ORDER BY
  1 DESC
LIMIT 5
