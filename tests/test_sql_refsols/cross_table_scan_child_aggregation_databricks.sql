WITH _s3 AS (
  SELECT
    o_custkey,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM tpch_s3_pq.tpch_sf1.orders
  GROUP BY
    1
)
SELECT
  orders.o_orderkey AS moment_id,
  customer.c_custkey AS customer_id,
  COALESCE(_s3.sum_o_totalprice, 0) AS total_ever
FROM tpch_s3_pq.tpch_sf1.orders AS orders
JOIN tpch_s3_pq.tpch_sf1.customer AS customer
  ON customer.c_custkey <= 2
LEFT JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
WHERE
  orders.o_orderkey IN (1, 2, 3)
ORDER BY
  1,
  2
