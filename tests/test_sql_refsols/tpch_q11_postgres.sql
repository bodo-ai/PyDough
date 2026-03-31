WITH _s0 AS (
  SELECT
    s_nationkey,
    s_suppkey
  FROM tpch.supplier
), _t2 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _s6 AS (
  SELECT
    COUNT(DISTINCT _s0.s_suppkey) AS ndistinct_s_suppkey
  FROM _s0 AS _s0
  JOIN _t2 AS _t2
    ON _s0.s_nationkey = _t2.n_nationkey
), _s7 AS (
  SELECT
    partsupp.ps_partkey,
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS sum_expr
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s2
    ON _s2.s_suppkey = partsupp.ps_suppkey
  JOIN _t2 AS _t4
    ON _s2.s_nationkey = _t4.n_nationkey
  GROUP BY
    1
)
SELECT
  _s7.ps_partkey AS PS_PARTKEY,
  COALESCE(_s7.sum_expr, 0) AS VALUE
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON (
    _s6.ndistinct_s_suppkey * 0.0001
  ) < COALESCE(_s7.sum_expr, 0)
ORDER BY
  2 DESC NULLS LAST
LIMIT 10
