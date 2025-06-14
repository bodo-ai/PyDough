WITH _s2 AS (
  SELECT
    SUM(ps_supplycost * ps_availqty) AS agg_0,
    ps_suppkey AS supplier_key
  FROM tpch.partsupp
  GROUP BY
    ps_suppkey
), _s0 AS (
  SELECT
    s_suppkey AS key,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t5 AS (
  SELECT
    n_nationkey AS key,
    n_name AS name
  FROM tpch.nation
), _t1 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0
  FROM _s2 AS _s2
  JOIN _s0 AS _s0
    ON _s0.key = _s2.supplier_key
  JOIN _t5 AS _t5
    ON _s0.nation_key = _t5.key AND _t5.name = 'GERMANY'
), _t6 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.key = partsupp.ps_suppkey
  JOIN _t5 AS _t9
    ON _s4.nation_key = _t9.key AND _t9.name = 'GERMANY'
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  _t6.part_key AS PS_PARTKEY,
  COALESCE(_t6.agg_1, 0) AS VALUE
FROM _t1 AS _t1
CROSS JOIN _t6 AS _t6
WHERE
  (
    COALESCE(_t1.agg_0, 0) * 0.0001
  ) < COALESCE(_t6.agg_1, 0)
ORDER BY
  value DESC
LIMIT 10
