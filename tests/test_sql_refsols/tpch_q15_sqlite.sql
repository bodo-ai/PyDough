WITH _t6 AS (
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
  FROM tpch.supplier AS supplier
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.supplier_key = supplier.s_suppkey
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
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_table_alias_5.agg_1, 0) AS TOTAL_REVENUE
FROM _table_alias_2 AS _table_alias_2
CROSS JOIN tpch.supplier AS supplier
LEFT JOIN _table_alias_5 AS _table_alias_5
  ON _table_alias_5.supplier_key = supplier.s_suppkey
WHERE
  _table_alias_2.max_revenue = COALESCE(_table_alias_5.agg_1, 0)
ORDER BY
  supplier.s_suppkey
