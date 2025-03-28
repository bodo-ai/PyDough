WITH _t7 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_shipdate AS ship_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate < CAST('1996-04-01' AS DATE)
    AND lineitem.l_shipdate >= CAST('1996-01-01' AS DATE)
), _table_alias_1 AS (
  SELECT
    SUM(_t7.extended_price * (
      1 - _t7.discount
    )) AS agg_0,
    _t7.supplier_key AS supplier_key
  FROM _t7 AS _t7
  GROUP BY
    _t7.supplier_key
), _table_alias_2 AS (
  SELECT
    MAX(COALESCE(_table_alias_1.agg_0, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.supplier_key = supplier.s_suppkey
), _table_alias_5 AS (
  SELECT
    SUM(_t10.extended_price * (
      1 - _t10.discount
    )) AS agg_1,
    _t10.supplier_key AS supplier_key
  FROM _t7 AS _t10
  GROUP BY
    _t10.supplier_key
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
