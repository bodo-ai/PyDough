WITH _s3 AS (
  SELECT
    CUSTOMER.c_nationkey,
    ORDERS.o_orderkey
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND ORDERS.o_orderkey = 454791
  WHERE
    CUSTOMER.c_custkey = 1
)
SELECT
  NATION.n_name AS name,
  _s3.o_orderkey AS okey
FROM tpch.NATION AS NATION
LEFT JOIN _s3 AS _s3
  ON NATION.n_nationkey = _s3.c_nationkey
