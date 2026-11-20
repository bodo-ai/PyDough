SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1994
  AND o_orderpriority = '1-URGENT'
