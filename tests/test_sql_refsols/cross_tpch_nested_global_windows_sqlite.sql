WITH _t1 AS (
  SELECT
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_totalprice,
    region.r_regionkey,
    SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey, region.r_regionkey) AS t
  FROM tpch.region AS region
  JOIN tpch.customer AS customer
    ON customer.c_custkey <= 2
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2
  WHERE
    region.r_regionkey <= 1
), _t0 AS (
  SELECT
    c_custkey,
    o_orderkey,
    o_totalprice,
    r_regionkey,
    t,
    SUM(o_totalprice) OVER (PARTITION BY r_regionkey) AS inner_total
  FROM _t1
)
SELECT
  r_regionkey AS rk,
  c_custkey AS k,
  o_orderkey AS oid,
  t,
  inner_total,
  SUM(o_totalprice) OVER () AS outer_total
FROM _t0
