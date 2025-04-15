WITH _s0 AS (
  SELECT
    s_suppkey AS key,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t5 AS (
  SELECT
    n_name AS name,
    n_nationkey AS key
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _t2 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_0
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s0
    ON _s0.key = partsupp.ps_suppkey
  JOIN _t5 AS _t5
    ON _s0.nation_key = _t5.key
), _s9 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.key = partsupp.ps_suppkey
  JOIN _t5 AS _t8
    ON _s4.nation_key = _t8.key
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  _s9.part_key AS PS_PARTKEY,
  COALESCE(_s9.agg_1, 0) AS VALUE
FROM _t2 AS _t2
LEFT JOIN _s9 AS _s9
  ON TRUE
WHERE
  (
    COALESCE(_t2.agg_0, 0) * 0.0001
  ) < COALESCE(_s9.agg_1, 0)
ORDER BY
  value DESC
LIMIT 10
