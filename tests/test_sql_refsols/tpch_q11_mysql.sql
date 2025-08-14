WITH _s0 AS (
  SELECT
    s_nationkey,
    s_suppkey
  FROM tpch.SUPPLIER
), _t3 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.NATION
  WHERE
    n_name = 'GERMANY'
), _s8 AS (
  SELECT
    SUM(PARTSUPP.ps_supplycost * PARTSUPP.ps_availqty) AS sum_metric
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s0 AS _s0
    ON PARTSUPP.ps_suppkey = _s0.s_suppkey
  JOIN _t3 AS _t3
    ON _s0.s_nationkey = _t3.n_nationkey
), _s9 AS (
  SELECT
    SUM(PARTSUPP.ps_supplycost * PARTSUPP.ps_availqty) AS sum_expr_2,
    PARTSUPP.ps_partkey
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s0 AS _s4
    ON PARTSUPP.ps_suppkey = _s4.s_suppkey
  JOIN _t3 AS _t5
    ON _s4.s_nationkey = _t5.n_nationkey
  GROUP BY
    PARTSUPP.ps_partkey
)
SELECT
  _s9.ps_partkey AS PS_PARTKEY,
  COALESCE(_s9.sum_expr_2, 0) AS VALUE
FROM _s8 AS _s8
JOIN _s9 AS _s9
  ON (
    COALESCE(_s8.sum_metric, 0) * 0.0001
  ) < COALESCE(_s9.sum_expr_2, 0)
ORDER BY
  COALESCE(_s9.sum_expr_2, 0) DESC
LIMIT 10
