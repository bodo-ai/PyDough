WITH _s1 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.orders
  WHERE
    o_custkey <= 2
)
SELECT
  orders.o_custkey AS c,
  orders.o_orderkey AS oid,
  LAG(orders.o_orderkey, 1) OVER (PARTITION BY _s1.o_custkey, customer.c_custkey ORDER BY orders.o_orderkey) AS prev_oid,
  LEAD(orders.o_orderkey, 1) OVER (PARTITION BY _s1.o_custkey, customer.c_custkey ORDER BY orders.o_orderkey) AS next_oid
FROM tpch.customer AS customer
CROSS JOIN _s1 AS _s1
JOIN tpch.orders AS orders
  ON _s1.o_custkey = orders.o_custkey AND orders.o_custkey <= 2
WHERE
  customer.c_custkey <= 2
