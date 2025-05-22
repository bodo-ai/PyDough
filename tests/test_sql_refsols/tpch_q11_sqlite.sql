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
), _t6 AS (
  SELECT
    n_nationkey AS key,
    n_name AS name
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _t2 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0
  FROM _s2 AS _s2
  JOIN _s0 AS _s0
    ON _s0.key = _s2.supplier_key
  JOIN _t6 AS _t6
    ON _s0.nation_key = _t6.key
), _s9 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _s0 AS _s4
    ON _s4.key = partsupp.ps_suppkey
  JOIN _t6 AS _t9
    ON _s4.nation_key = _t9.key
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
