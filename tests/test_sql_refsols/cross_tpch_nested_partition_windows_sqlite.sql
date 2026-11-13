WITH _s3 AS (
  SELECT DISTINCT
    o_custkey
  FROM tpch.orders
  WHERE
    o_custkey <= 2
), _t1 AS (
  SELECT
    orders.o_custkey AS c,
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_totalprice,
    region.r_regionkey,
    SUM(orders.o_totalprice) OVER (
      PARTITION BY _s3.o_custkey, customer.c_custkey, region.r_regionkey
      ORDER BY orders.o_orderkey
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running
  FROM tpch.region AS region
  JOIN tpch.customer AS customer
    ON customer.c_custkey <= 2
  CROSS JOIN _s3 AS _s3
  JOIN tpch.orders AS orders
    ON _s3.o_custkey = orders.o_custkey AND orders.o_custkey <= 2
  WHERE
    region.r_regionkey <= 1
), _t0 AS (
  SELECT
    c,
    c_custkey,
    o_orderkey,
    o_totalprice,
    r_regionkey,
    running,
    SUM(o_totalprice) OVER (PARTITION BY r_regionkey) AS inner_total
  FROM _t1
)
SELECT
  r_regionkey AS r,
  c_custkey AS k,
  c,
  o_orderkey AS oid,
  running,
  inner_total,
  SUM(o_totalprice) OVER () AS outer_total
FROM _t0
