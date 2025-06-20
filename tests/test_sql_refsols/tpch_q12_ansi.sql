WITH _t1 AS (
  SELECT
    SUM(_s1.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS agg_0,
    SUM(NOT _s1.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS agg_1,
    _s0.l_shipmode AS ship_mode
  FROM tpch.lineitem AS _s0
  JOIN tpch.orders AS _s1
    ON _s0.l_orderkey = _s1.o_orderkey
  WHERE
    EXTRACT(YEAR FROM _s0.l_receiptdate) = 1994
    AND _s0.l_commitdate < _s0.l_receiptdate
    AND _s0.l_commitdate > _s0.l_shipdate
    AND (
      _s0.l_shipmode = 'MAIL' OR _s0.l_shipmode = 'SHIP'
    )
  GROUP BY
    _s0.l_shipmode
)
SELECT
  ship_mode AS L_SHIPMODE,
  COALESCE(agg_0, 0) AS HIGH_LINE_COUNT,
  COALESCE(agg_1, 0) AS LOW_LINE_COUNT
FROM _t1
ORDER BY
  l_shipmode
