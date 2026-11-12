WITH _s3 AS (
  SELECT
    customer.c_nationkey,
    orders.o_orderkey
  FROM tpch_s3_pq.tpch_sf1.customer AS customer
  JOIN tpch_s3_pq.tpch_sf1.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_orderkey = 454791
  WHERE
    customer.c_custkey = 1
)
SELECT
  nation.n_name AS name,
  _s3.o_orderkey AS okey
FROM tpch_s3_pq.tpch_sf1.nation AS nation
LEFT JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
