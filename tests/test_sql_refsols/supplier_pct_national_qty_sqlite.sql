WITH _s0 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS nation_name,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
), _t2 AS (
  SELECT
    region.r_regionkey AS key,
    region.r_name AS name
  FROM tpch.region AS region
), _s1 AS (
  SELECT
    _t2.key AS key
  FROM _t2 AS _t2
  WHERE
    _t2.name = 'AFRICA'
), _s2 AS (
  SELECT
    _s0.key AS key,
    _s0.nation_name AS nation_name
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.region_key = _s1.key
    )
), _t3 AS (
  SELECT
    supplier.s_acctbal AS account_balance,
    supplier.s_comment AS comment,
    supplier.s_suppkey AS key,
    supplier.s_name AS name,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _s3 AS (
  SELECT
    _t3.key AS key,
    _t3.name AS name,
    _t3.nation_key AS nation_key
  FROM _t3 AS _t3
  WHERE
    _t3.account_balance >= 0.0 AND _t3.comment LIKE '%careful%'
), _s6 AS (
  SELECT
    _s2.key AS key,
    _s3.key AS key_2,
    _s3.name AS name_3,
    _s2.nation_name AS nation_name
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s2.key = _s3.nation_key
), _t6 AS (
  SELECT
    lineitem.l_partkey AS part_key,
    lineitem.l_quantity AS quantity,
    lineitem.l_shipdate AS ship_date,
    lineitem.l_shipmode AS ship_mode,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _t5 AS (
  SELECT
    _t6.part_key AS part_key,
    _t6.quantity AS quantity,
    _t6.supplier_key AS supplier_key
  FROM _t6 AS _t6
  WHERE
    CAST(STRFTIME('%Y', _t6.ship_date) AS INTEGER) = 1995 AND _t6.ship_mode = 'SHIP'
), _s4 AS (
  SELECT
    SUM(_t5.quantity) AS agg_0,
    _t5.part_key AS part_key,
    _t5.supplier_key AS supplier_key
  FROM _t5 AS _t5
  GROUP BY
    _t5.part_key,
    _t5.supplier_key
), _t7 AS (
  SELECT
    part.p_container AS container,
    part.p_partkey AS key,
    part.p_name AS name
  FROM tpch.part AS part
), _s5 AS (
  SELECT
    _t7.key AS key
  FROM _t7 AS _t7
  WHERE
    _t7.container LIKE 'LG%' AND _t7.name LIKE '%tomato%'
), _t4 AS (
  SELECT
    _s4.agg_0 AS agg_0,
    _s4.supplier_key AS supplier_key
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.part_key = _s5.key
), _s7 AS (
  SELECT
    SUM(_t4.agg_0) AS agg_0,
    _t4.supplier_key AS supplier_key
  FROM _t4 AS _t4
  GROUP BY
    _t4.supplier_key
), _t1 AS (
  SELECT
    _s7.agg_0 AS agg_0,
    _s6.key AS key,
    _s6.name_3 AS name_3,
    _s6.nation_name AS nation_name
  FROM _s6 AS _s6
  LEFT JOIN _s7 AS _s7
    ON _s6.key_2 = _s7.supplier_key
), _t0 AS (
  SELECT
    CAST((
      100.0 * COALESCE(_t1.agg_0, 0)
    ) AS REAL) / SUM(COALESCE(_t1.agg_0, 0)) OVER (PARTITION BY _t1.key) AS national_qty_pct,
    _t1.name_3 AS supplier_name,
    COALESCE(_t1.agg_0, 0) AS supplier_quantity,
    _t1.nation_name AS nation_name
  FROM _t1 AS _t1
)
SELECT
  _t0.supplier_name AS supplier_name,
  _t0.nation_name AS nation_name,
  _t0.supplier_quantity AS supplier_quantity,
  _t0.national_qty_pct AS national_qty_pct
FROM _t0 AS _t0
ORDER BY
  national_qty_pct DESC
LIMIT 5
