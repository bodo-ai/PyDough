SELECT
  o_orderdate AS order_date,
  o_orderkey,
  o_totalprice
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_totalprice < 1000.0
