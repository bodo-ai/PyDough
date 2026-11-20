WITH _t1 AS (
  SELECT
    n_nationkey
  FROM tpch.NATION
  WHERE
    n_nationkey <= 4
), _s3 AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice) AS sum_l_extendedprice
  FROM tpch.LINEITEM
  GROUP BY
    1
), _s5 AS (
  SELECT
    l_partkey,
    COUNT(*) AS n_rows
  FROM tpch.LINEITEM
  GROUP BY
    1
), _s7 AS (
  SELECT
    LINEITEM.l_orderkey,
    SUM(_s5.n_rows) AS sum_n_rows
  FROM tpch.LINEITEM AS LINEITEM
  LEFT JOIN _s5 AS _s5
    ON LINEITEM.l_partkey = _s5.l_partkey
  GROUP BY
    1
), _s9 AS (
  SELECT
    COALESCE(SUM(_s3.sum_l_extendedprice), 0) AS sum_order_value,
    COALESCE(SUM(_s7.sum_n_rows), 0) AS sum_part_usage_score,
    _t4.n_nationkey
  FROM _t1 AS _t4
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_orderkey <= _t4.n_nationkey AND ORDERS.o_orderkey = 1
  JOIN _s3 AS _s3
    ON ORDERS.o_orderkey = _s3.l_orderkey
  JOIN _s7 AS _s7
    ON ORDERS.o_orderkey = _s7.l_orderkey
  GROUP BY
    3
)
SELECT
  _t1.n_nationkey AS k,
  COALESCE(_s9.sum_order_value, 0) AS total_value,
  COALESCE(_s9.sum_part_usage_score, 0) AS part_usage_score
FROM _t1 AS _t1
LEFT JOIN _s9 AS _s9
  ON _s9.n_nationkey = _t1.n_nationkey
ORDER BY
  1
