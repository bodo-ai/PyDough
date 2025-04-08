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
    n_name AS name,
    n_nationkey AS key
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
), _s6 AS (
  SELECT
    SUM(ps_supplycost * ps_availqty) AS agg_1,
    ps_partkey AS part_key,
    ps_suppkey AS supplier_key
  FROM tpch.partsupp
  GROUP BY
    ps_partkey,
    ps_suppkey
), _s9 AS (
  SELECT
    SUM(_s6.agg_1) AS agg_1,
    _s6.part_key
  FROM _s6 AS _s6
  JOIN _s0 AS _s4
    ON _s4.key = _s6.supplier_key
  JOIN _t6 AS _t10
    ON _s4.nation_key = _t10.key
  GROUP BY
    _s6.part_key
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
