WITH _t2 AS (
  SELECT
    customer.c_custkey,
    orders.o_orderkey,
    SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey) AS tot
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2
  WHERE
    customer.c_custkey <= 2
), _t AS (
  SELECT
    c_custkey,
    o_orderkey,
    tot,
    ROW_NUMBER() OVER (PARTITION BY c_custkey ORDER BY o_orderkey) AS _w
  FROM _t2
)
SELECT
  c_custkey AS k,
  o_orderkey AS oid,
  tot
FROM _t
WHERE
  _w <= 2
ORDER BY
  2
