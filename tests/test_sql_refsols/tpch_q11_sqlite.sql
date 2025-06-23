WITH _s0 AS (
  SELECT
    s_nationkey,
    s_suppkey
  FROM tpch.supplier
), _t4 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _t1 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS sum_metric
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s0
    ON _s0.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t4
    ON _s0.s_nationkey = _t4.n_nationkey
), _t5 AS (
  SELECT
    partsupp.ps_partkey AS part_key,
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS sum_expr_2
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t8
    ON _s4.s_nationkey = _t8.n_nationkey
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  _t5.part_key AS PS_PARTKEY,
  COALESCE(_t5.sum_expr_2, 0) AS VALUE
FROM _t1 AS _t1
JOIN _t5 AS _t5
  ON (
    COALESCE(_t1.sum_metric, 0) * 0.0001
  ) < COALESCE(_t5.sum_expr_2, 0)
ORDER BY
  value DESC
LIMIT 10
