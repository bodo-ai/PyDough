WITH _table_alias_2 AS (
  SELECT
    partsupp.ps_availqty AS availqty,
    partsupp.ps_suppkey AS supplier_key,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.partsupp AS partsupp
), _table_alias_0 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _t7 AS (
  SELECT
    nation.n_name AS name,
    nation.n_nationkey AS key
  FROM tpch.nation AS nation
  WHERE
    nation.n_name = 'GERMANY'
), _table_alias_1 AS (
  SELECT
    _t7.key AS key
  FROM _t7 AS _t7
), _table_alias_3 AS (
  SELECT
    _table_alias_0.key AS key
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.nation_key = _table_alias_1.key
), _t4 AS (
  SELECT
    SUM(_table_alias_2.supplycost * _table_alias_2.availqty) AS agg_0
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.supplier_key = _table_alias_3.key
), _table_alias_8 AS (
  SELECT
    COALESCE(_t4.agg_0, 0) * 0.0001 AS min_market_share
  FROM _t4 AS _t4
), _table_alias_6 AS (
  SELECT
    partsupp.ps_availqty AS availqty,
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.partsupp AS partsupp
), _table_alias_5 AS (
  SELECT
    _t10.key AS key
  FROM _t7 AS _t10
), _table_alias_7 AS (
  SELECT
    _table_alias_4.key AS key
  FROM _table_alias_0 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.nation_key = _table_alias_5.key
), _table_alias_9 AS (
  SELECT
    SUM(_table_alias_6.supplycost * _table_alias_6.availqty) AS agg_1,
    _table_alias_6.part_key AS part_key
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.supplier_key = _table_alias_7.key
  GROUP BY
    _table_alias_6.part_key
), _t0 AS (
  SELECT
    _table_alias_9.part_key AS ps_partkey,
    COALESCE(_table_alias_9.agg_1, 0) AS value,
    COALESCE(_table_alias_9.agg_1, 0) AS ordering_2
  FROM _table_alias_8 AS _table_alias_8
  LEFT JOIN _table_alias_9 AS _table_alias_9
    ON TRUE
  WHERE
    _table_alias_8.min_market_share < COALESCE(_table_alias_9.agg_1, 0)
  ORDER BY
    ordering_2 DESC
  LIMIT 10
)
SELECT
  _t0.ps_partkey AS PS_PARTKEY,
  _t0.value AS VALUE
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
