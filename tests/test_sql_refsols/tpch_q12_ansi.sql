WITH _table_alias_0 AS (
  SELECT
    lineitem.l_orderkey AS order_key,
    lineitem.l_shipmode AS ship_mode
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_commitdate > lineitem.l_shipdate
    AND lineitem.l_receiptdate < CAST('1995-01-01' AS DATE)
    AND lineitem.l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND (
      lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
    )
), _table_alias_1 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderpriority AS order_priority
  FROM tpch.orders AS orders
), _t1 AS (
  SELECT
    SUM(
      _table_alias_1.order_priority = '1-URGENT'
      OR _table_alias_1.order_priority = '2-HIGH'
    ) AS agg_0,
    SUM(
      (
        _table_alias_1.order_priority <> '1-URGENT'
        AND _table_alias_1.order_priority <> '2-HIGH'
      )
    ) AS agg_1,
    _table_alias_0.ship_mode AS ship_mode
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.order_key = _table_alias_1.key
  GROUP BY
    _table_alias_0.ship_mode
)
SELECT
  _t1.ship_mode AS L_SHIPMODE,
  COALESCE(_t1.agg_0, 0) AS HIGH_LINE_COUNT,
  COALESCE(_t1.agg_1, 0) AS LOW_LINE_COUNT
FROM _t1 AS _t1
ORDER BY
  _t1.ship_mode
