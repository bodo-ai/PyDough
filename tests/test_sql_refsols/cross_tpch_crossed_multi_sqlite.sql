WITH _s1 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.orders
  WHERE
    o_custkey <= 2
), _t1 AS (
  SELECT
    orders.o_custkey AS c,
    customer.c_custkey,
    _s1.o_custkey,
    orders.o_orderkey,
    SUM(orders.o_totalprice) OVER (
      PARTITION BY _s1.o_custkey, customer.c_custkey
      ORDER BY orders.o_orderkey
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running
  FROM tpch.customer AS customer
  CROSS JOIN _s1 AS _s1
  JOIN tpch.orders AS orders
    ON _s1.o_custkey = orders.o_custkey AND orders.o_custkey <= 2
  WHERE
    customer.c_custkey <= 2
), _t0 AS (
  SELECT
    c,
    c_custkey,
    o_orderkey,
    running,
    LAG(o_orderkey, 1) OVER (PARTITION BY o_custkey, c_custkey ORDER BY o_orderkey) AS prev_oid
  FROM _t1
)
SELECT
  c,
  o_orderkey AS oid,
  running,
  prev_oid
FROM _t0
WHERE
  c = c_custkey
