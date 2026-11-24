WITH _s1 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.orders
  WHERE
    o_custkey <= 2
), _t0 AS (
  SELECT
    orders.o_custkey AS c,
    customer.c_custkey,
    orders.o_orderkey,
    LEAD(orders.o_orderkey, 1) OVER (PARTITION BY _s1.o_custkey, customer.c_custkey ORDER BY orders.o_orderkey) AS next_oid,
    LAG(orders.o_orderkey, 1) OVER (PARTITION BY _s1.o_custkey, customer.c_custkey ORDER BY orders.o_orderkey) AS prev_oid
  FROM tpch.customer AS customer
  CROSS JOIN _s1 AS _s1
  JOIN tpch.orders AS orders
    ON _s1.o_custkey = orders.o_custkey AND orders.o_custkey <= 2
  WHERE
    customer.c_custkey <= 2
)
SELECT
  c,
  o_orderkey AS oid,
  prev_oid,
  next_oid
FROM _t0
WHERE
  c = c_custkey
