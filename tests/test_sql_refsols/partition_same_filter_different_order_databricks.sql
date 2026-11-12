SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_a,
  COALESCE(SUM(o_totalprice), 0) AS total_b
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1995
