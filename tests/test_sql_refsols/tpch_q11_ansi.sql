WITH _t1 AS (
  SELECT
    SUM(_s0.ps_supplycost * _s0.ps_availqty) AS agg_0
  FROM tpch.partsupp AS _s0
  JOIN tpch.supplier AS _s1
    ON _s0.ps_suppkey = _s1.s_suppkey
  JOIN tpch.nation AS _s2
    ON _s1.s_nationkey = _s2.n_nationkey AND _s2.n_name = 'GERMANY'
), _t5 AS (
  SELECT
    SUM(_s7.ps_supplycost * _s7.ps_availqty) AS agg_1,
    _s7.ps_partkey AS part_key
  FROM tpch.partsupp AS _s7
  JOIN tpch.supplier AS _s8
    ON _s7.ps_suppkey = _s8.s_suppkey
  JOIN tpch.nation AS _s9
    ON _s8.s_nationkey = _s9.n_nationkey AND _s9.n_name = 'GERMANY'
  GROUP BY
    _s7.ps_partkey
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
