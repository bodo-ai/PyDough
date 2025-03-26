WITH _table_alias_0 AS (
  SELECT
    supplier.s_address AS s_address,
    supplier.s_name AS s_name,
    supplier.s_nationkey AS nation_key,
    supplier.s_suppkey AS key
  FROM tpch.supplier AS supplier
), _table_alias_1 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _table_alias_6 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_1.name AS name_3,
    _table_alias_0.s_address AS s_address,
    _table_alias_0.s_name AS s_name
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.nation_key = _table_alias_1.key
), _table_alias_2 AS (
  SELECT
    partsupp.ps_availqty AS availqty,
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
), _table_alias_3 AS (
  SELECT
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    part.p_name LIKE 'forest%'
), _table_alias_4 AS (
  SELECT
    _table_alias_2.availqty AS availqty,
    _table_alias_3.key AS key,
    _table_alias_2.supplier_key AS supplier_key
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.part_key = _table_alias_3.key
), _table_alias_5 AS (
  SELECT
    SUM(lineitem.l_quantity) AS agg_0,
    lineitem.l_partkey AS part_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate < '1995-01-01' AND lineitem.l_shipdate >= '1994-01-01'
  GROUP BY
    lineitem.l_partkey
), _table_alias_7 AS (
  SELECT
    COUNT() AS agg_0,
    _table_alias_4.supplier_key AS supplier_key
  FROM _table_alias_4 AS _table_alias_4
  LEFT JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.key = _table_alias_5.part_key
  WHERE
    _table_alias_4.availqty > (
      COALESCE(_table_alias_5.agg_0, 0) * 0.5
    )
  GROUP BY
    _table_alias_4.supplier_key
), _t0 AS (
  SELECT
    _table_alias_6.s_address AS s_address,
    _table_alias_6.s_name AS s_name,
    _table_alias_6.s_name AS ordering_1
  FROM _table_alias_6 AS _table_alias_6
  LEFT JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.key = _table_alias_7.supplier_key
  WHERE
    (
      COALESCE(_table_alias_7.agg_0 <> 0, FALSE) AND _table_alias_6.name_3 = 'CANADA'
    ) > 0
  ORDER BY
    ordering_1
  LIMIT 10
)
SELECT
  _t0.s_name AS S_NAME,
  _t0.s_address AS S_ADDRESS
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1
