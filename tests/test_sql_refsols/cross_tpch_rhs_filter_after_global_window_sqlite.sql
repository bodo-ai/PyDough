WITH _t0 AS (
  SELECT
    customer.c_custkey,
    orders.o_custkey,
    orders.o_orderkey,
    SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey) AS total
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2
  WHERE
    customer.c_custkey <= 2
)
SELECT
  c_custkey AS k,
  o_orderkey AS oid,
  total
FROM _t0
WHERE
  c_custkey = o_custkey
