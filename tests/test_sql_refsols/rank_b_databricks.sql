SELECT
  o_orderkey AS order_key,
  RANK() OVER (ORDER BY o_orderpriority NULLS LAST) AS rank
FROM tpch_s3_pq.tpch_sf1.orders
