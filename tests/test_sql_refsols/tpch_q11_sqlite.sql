WITH _t0 AS (
  SELECT
    s_suppkey AS key,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t7 AS (
  SELECT
    n_name AS name,
    n_nationkey AS key
  FROM tpch.nation
  WHERE
    n_name = 'GERMANY'
), _t4 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_0
  FROM tpch.partsupp AS partsupp
  JOIN _t0 AS _t0
    ON _t0.key = partsupp.ps_suppkey
  JOIN _t7 AS _t7
    ON _t0.nation_key = _t7.key
), _t9_2 AS (
  SELECT
    SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS agg_1,
    partsupp.ps_partkey AS part_key
  FROM tpch.partsupp AS partsupp
  JOIN _t0 AS _t4
    ON _t4.key = partsupp.ps_suppkey
  JOIN _t7 AS _t10
    ON _t10.key = _t4.nation_key
  GROUP BY
    partsupp.ps_partkey
), _t0_2 AS (
  SELECT
    _t9.part_key AS ps_partkey,
    COALESCE(_t9.agg_1, 0) AS value,
    COALESCE(_t9.agg_1, 0) AS ordering_2
  FROM _t4 AS _t4
  LEFT JOIN _t9_2 AS _t9
    ON TRUE
  WHERE
    (
      COALESCE(_t4.agg_0, 0) * 0.0001
    ) < COALESCE(_t9.agg_1, 0)
  ORDER BY
    ordering_2 DESC
  LIMIT 10
)
SELECT
  _t0.ps_partkey AS PS_PARTKEY,
  _t0.value AS VALUE
FROM _t0_2 AS _t0
ORDER BY
  _t0.ordering_2 DESC
