WITH _t AS (
  SELECT
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY customer.c_custkey ORDER BY orders.o_orderkey) AS _w
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2
  WHERE
    customer.c_custkey <= 2
)
SELECT
  c_custkey AS k,
  o_orderkey AS oid,
  SUM(o_totalprice) OVER (PARTITION BY c_custkey) AS tot
FROM _t
WHERE
  _w <= 2
ORDER BY
  2
