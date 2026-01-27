WITH _t3 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
), _s0 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _t3
), _s2 AS (
  SELECT
    CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          orders.o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(orders.o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          _s0.min_o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(_s0.min_o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) AS ord_wk,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN tpch.orders AS orders
    ON CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          orders.o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(orders.o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          _s0.min_o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(_s0.min_o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) < 10
    AND orders.o_orderpriority = '1-URGENT'
    AND orders.o_orderstatus = 'F'
  GROUP BY
    1
), _s3 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _t3
), _t0 AS (
  SELECT
    CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          lineitem.l_receiptdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(lineitem.l_receiptdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          _s3.min_o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(_s3.min_o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) AS line_wk,
    _s2.ord_wk,
    MAX(_s2.n_rows) AS anything_n_rows,
    COUNT(*) AS n_rows
  FROM _s2 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN tpch.lineitem AS lineitem
    ON CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          lineitem.l_receiptdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(lineitem.l_receiptdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          _s3.min_o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(_s3.min_o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) < 10
    AND CAST(STRFTIME('%Y', lineitem.l_receiptdate) AS INTEGER) = 1992
    AND _s2.ord_wk = CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          lineitem.l_receiptdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(lineitem.l_receiptdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          _s3.min_o_orderdate,
          '-' || CAST(CAST(STRFTIME('%w', DATETIME(_s3.min_o_orderdate)) AS INTEGER) AS TEXT) || ' days',
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER)
    AND lineitem.l_returnflag = 'R'
    AND lineitem.l_shipmode = 'RAIL'
  GROUP BY
    1,
    2
)
SELECT
  ord_wk AS wk,
  n_rows AS n_lines,
  anything_n_rows AS n_orders,
  ROUND(
    CAST(SUM(n_rows) OVER (ORDER BY line_wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(anything_n_rows) OVER (ORDER BY ord_wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    4
  ) AS lpo
FROM _t0
ORDER BY
  1
