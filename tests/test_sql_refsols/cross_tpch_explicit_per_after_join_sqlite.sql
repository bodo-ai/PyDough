WITH _s1 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.orders
  WHERE
    o_custkey <= 2
)
SELECT
  customer.c_custkey AS k,
  orders.o_custkey AS c,
  orders.o_orderkey AS oid,
  SUM(orders.o_totalprice) OVER (PARTITION BY _s1.o_custkey) AS total
FROM tpch.customer AS customer
CROSS JOIN _s1 AS _s1
JOIN tpch.orders AS orders
  ON _s1.o_custkey = orders.o_custkey AND orders.o_custkey <= 2
WHERE
  customer.c_custkey <= 2
