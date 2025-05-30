WITH _s3 AS (
  SELECT
    orders.o_orderkey AS key_2,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_orderkey = 454791
  WHERE
    customer.c_custkey = 1
)
SELECT
  nation.n_name AS name,
  _s3.key_2 AS okey
FROM tpch.nation AS nation
LEFT JOIN _s3 AS _s3
  ON _s3.nation_key = nation.n_nationkey
