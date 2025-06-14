WITH _s0 AS (
  SELECT
    s_suppkey AS key,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t4 AS (
  SELECT
    n_nationkey AS key,
    n_name AS name
  FROM tpch.nation
), _t1 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_0
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s0
    ON _s0.key = partsupp.ps_suppkey
  JOIN _t4 AS _t4
    ON _s0.nation_key = _t4.key AND _t4.name = 'GERMANY'
), _t5 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.key = partsupp.ps_suppkey
  JOIN _t4 AS _t8
    ON _s4.nation_key = _t8.key AND _t8.name = 'GERMANY'
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
