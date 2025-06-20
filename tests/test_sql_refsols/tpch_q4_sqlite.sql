WITH _t0 AS (
  SELECT
    COUNT(*) AS order_count,
    _s0.o_orderpriority AS o_orderpriority
  FROM tpch.orders AS _s0
  WHERE
    CASE
      WHEN CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', _s0.o_orderdate) AS INTEGER) >= 10
      THEN 4
    END = 3
    AND CAST(STRFTIME('%Y', _s0.o_orderdate) AS INTEGER) = 1993
    AND EXISTS(
      SELECT
        1 AS "1"
      FROM tpch.lineitem AS _s1
      WHERE
        _s0.o_orderkey = _s1.l_orderkey AND _s1.l_commitdate < _s1.l_receiptdate
    )
  GROUP BY
    _s0.o_orderpriority
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  o_orderpriority
