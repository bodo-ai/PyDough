WITH _t1 AS (
  SELECT
    n_nationkey
  FROM tpch.nation
  WHERE
    n_nationkey <= 4
), _t5 AS (
  SELECT
    r_regionkey
  FROM tpch.region
  WHERE
    r_regionkey <= 2
), _s7 AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice) AS sum_l_extendedprice
  FROM tpch.lineitem
  GROUP BY
    1
), _s9 AS (
  SELECT
    COALESCE(SUM(_s7.sum_l_extendedprice), 0) AS sum_order_value,
    _t8.n_nationkey,
    _t9.r_regionkey
  FROM _t1 AS _t8
  CROSS JOIN _t5 AS _t9
  JOIN tpch.orders AS orders
    ON _t9.r_regionkey >= orders.o_orderkey AND orders.o_orderkey = 1
  JOIN _s7 AS _s7
    ON _s7.l_orderkey = orders.o_orderkey
  GROUP BY
    2,
    3
), _s11 AS (
  SELECT
    COALESCE(SUM(_s9.sum_order_value), 0) AS sum_amount,
    _t4.n_nationkey
  FROM _t1 AS _t4
  JOIN _t5 AS _t5
    ON _t4.n_nationkey >= _t5.r_regionkey
  LEFT JOIN _s9 AS _s9
    ON _s9.n_nationkey = _t4.n_nationkey AND _s9.r_regionkey = _t5.r_regionkey
  GROUP BY
    2
)
SELECT
  _t1.n_nationkey AS k,
  COALESCE(_s11.sum_amount, 0) AS total_value
FROM _t1 AS _t1
LEFT JOIN _s11 AS _s11
  ON _s11.n_nationkey = _t1.n_nationkey
ORDER BY
  1
