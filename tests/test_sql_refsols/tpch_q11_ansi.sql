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
), _t1 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_0
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s0
    ON _s0.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t4
    ON _s0.s_nationkey = _t4.n_nationkey AND _t4.n_name = 'GERMANY'
), _t5 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.s_suppkey = partsupp.ps_suppkey
  JOIN _t4 AS _t8
    ON _s4.s_nationkey = _t8.n_nationkey AND _t8.n_name = 'GERMANY'
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  _t5.part_key AS PS_PARTKEY,
  COALESCE(_t5.agg_1, 0) AS VALUE
FROM _t1 AS _t1
CROSS JOIN _t5 AS _t5
WHERE
  (
    COALESCE(_t1.agg_0, 0) * 0.0001
  ) < COALESCE(_t5.agg_1, 0)
ORDER BY
  value DESC
LIMIT 10
