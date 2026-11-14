WITH _t1 AS (
  SELECT
    n_nationkey
  FROM tpch.nation
  WHERE
    n_nationkey <= 1
), _t2 AS (
  SELECT
    c_custkey
  FROM tpch.customer
  WHERE
    c_custkey <= 2
), _t4 AS (
  SELECT
    _t7.c_custkey,
    _t6.n_nationkey,
    orders.o_custkey,
    SUM(orders.o_totalprice) OVER (PARTITION BY _t6.n_nationkey, _t7.c_custkey) AS t
  FROM _t1 AS _t6
  CROSS JOIN _t2 AS _t7
  JOIN tpch.orders AS orders
    ON _t7.c_custkey = orders.o_custkey
), _s7 AS (
  SELECT
    c_custkey,
    n_nationkey,
    SUM(t) AS sum_t
  FROM _t4
  WHERE
    c_custkey = o_custkey
  GROUP BY
    1,
    2
)
SELECT
  _t1.n_nationkey AS nk,
  _t2.c_custkey AS ck,
  COALESCE(_s7.sum_t, 0) AS total
FROM _t1 AS _t1
CROSS JOIN _t2 AS _t2
LEFT JOIN _s7 AS _s7
  ON _s7.c_custkey = _t2.c_custkey AND _s7.n_nationkey = _t1.n_nationkey
