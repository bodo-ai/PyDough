WITH _table_alias_0 AS (
  SELECT
    supplier.s_suppkey AS key
  FROM tpch.supplier AS supplier
), _t6 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_shipdate AS ship_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate < '1996-04-01' AND lineitem.l_shipdate >= '1996-01-01'
), _table_alias_1 AS (
  SELECT
    SUM(_t6.extended_price * (
      1 - _t6.discount
    )) AS agg_0,
    _t6.supplier_key AS supplier_key
  FROM _t6 AS _t6
  GROUP BY
    _t6.supplier_key
), _table_alias_2 AS (
  SELECT
    MAX(COALESCE(_table_alias_1.agg_0, 0)) AS max_revenue
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.supplier_key
), _table_alias_3 AS (
  SELECT
    supplier.s_address AS address,
    supplier.s_suppkey AS key,
    supplier.s_name AS name,
    supplier.s_phone AS phone
  FROM tpch.supplier AS supplier
), _table_alias_4 AS (
  SELECT
    _table_alias_3.address AS address,
    _table_alias_3.key AS key,
    _table_alias_2.max_revenue AS max_revenue,
    _table_alias_3.name AS name,
    _table_alias_3.phone AS phone
  FROM _table_alias_2 AS _table_alias_2
  CROSS JOIN _table_alias_3 AS _table_alias_3
), _table_alias_5 AS (
  SELECT
    SUM(_t8.extended_price * (
      1 - _t8.discount
    )) AS agg_1,
    _t8.supplier_key AS supplier_key
  FROM _t6 AS _t8
  GROUP BY
    _t8.supplier_key
)
SELECT
  _table_alias_4.key AS S_SUPPKEY,
  _table_alias_4.name AS S_NAME,
  _table_alias_4.address AS S_ADDRESS,
  _table_alias_4.phone AS S_PHONE,
  COALESCE(_table_alias_5.agg_1, 0) AS TOTAL_REVENUE
FROM _table_alias_4 AS _table_alias_4
LEFT JOIN _table_alias_5 AS _table_alias_5
  ON _table_alias_4.key = _table_alias_5.supplier_key
WHERE
  _table_alias_4.max_revenue = COALESCE(_table_alias_5.agg_1, 0)
ORDER BY
  _table_alias_4.key
