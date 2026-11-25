SELECT
  o_orderkey AS oid,
  o_totalprice AS amount,
  ROW_NUMBER() OVER (ORDER BY o_totalprice DESC NULLS FIRST, o_orderkey NULLS LAST) AS r
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_custkey <= 2
ORDER BY
  2 DESC,
  1
LIMIT 5
