SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM tpch_s3_pq.tpch_sf1.orders
ORDER BY
  2 DESC,
  1
LIMIT 5
