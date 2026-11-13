WITH _t1 AS (
  SELECT
    c_custkey
  FROM tpch.customer
  WHERE
    c_custkey <= 2
), _t2 AS (
  SELECT
    _t4.c_custkey,
    SUM(orders.o_totalprice) OVER (PARTITION BY region.r_regionkey, _t4.c_custkey, _t4.c_custkey) AS t
  FROM _t1 AS _t4
  JOIN tpch.region AS region
    ON region.r_regionkey <= 1
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2
), _s5 AS (
  SELECT
    c_custkey,
    SUM(t) AS sum_t
  FROM _t2
  GROUP BY
    1
)
SELECT
  _t1.c_custkey AS k,
  COALESCE(_s5.sum_t, 0) AS total
FROM _t1 AS _t1
LEFT JOIN _s5 AS _s5
  ON _s5.c_custkey = _t1.c_custkey
