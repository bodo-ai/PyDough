WITH _t3 AS (
  SELECT
    o_orderdate
  FROM tpch.ORDERS
), _s0 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _t3
), _s2 AS (
  SELECT
    CAST((
      DATEDIFF(CAST(ORDERS.o_orderdate AS DATETIME), CAST(_s0.min_o_orderdate AS DATETIME)) + (
        (
          DAYOFWEEK(_s0.min_o_orderdate) + -1
        ) % 7
      ) - (
        (
          DAYOFWEEK(ORDERS.o_orderdate) + -1
        ) % 7
      )
    ) / 7 AS SIGNED) AS ord_wk,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN tpch.ORDERS AS ORDERS
    ON CAST((
      DATEDIFF(CAST(ORDERS.o_orderdate AS DATETIME), CAST(_s0.min_o_orderdate AS DATETIME)) + (
        (
          DAYOFWEEK(_s0.min_o_orderdate) + -1
        ) % 7
      ) - (
        (
          DAYOFWEEK(ORDERS.o_orderdate) + -1
        ) % 7
      )
    ) / 7 AS SIGNED) < 10
    AND ORDERS.o_orderpriority = '1-URGENT'
    AND ORDERS.o_orderstatus = 'F'
  GROUP BY
    1
), _s3 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _t3
), _t0 AS (
  SELECT
    CAST((
      DATEDIFF(CAST(LINEITEM.l_receiptdate AS DATETIME), CAST(_s3.min_o_orderdate AS DATETIME)) + (
        (
          DAYOFWEEK(_s3.min_o_orderdate) + -1
        ) % 7
      ) - (
        (
          DAYOFWEEK(LINEITEM.l_receiptdate) + -1
        ) % 7
      )
    ) / 7 AS SIGNED) AS line_wk,
    _s2.ord_wk,
    ANY_VALUE(_s2.n_rows) AS anything_n_rows,
    COUNT(*) AS n_rows
  FROM _s2 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN tpch.LINEITEM AS LINEITEM
    ON CAST((
      DATEDIFF(CAST(LINEITEM.l_receiptdate AS DATETIME), CAST(_s3.min_o_orderdate AS DATETIME)) + (
        (
          DAYOFWEEK(_s3.min_o_orderdate) + -1
        ) % 7
      ) - (
        (
          DAYOFWEEK(LINEITEM.l_receiptdate) + -1
        ) % 7
      )
    ) / 7 AS SIGNED) < 10
    AND EXTRACT(YEAR FROM CAST(LINEITEM.l_receiptdate AS DATETIME)) = 1992
    AND LINEITEM.l_returnflag = 'R'
    AND LINEITEM.l_shipmode = 'RAIL'
    AND _s2.ord_wk = CAST((
      DATEDIFF(CAST(LINEITEM.l_receiptdate AS DATETIME), CAST(_s3.min_o_orderdate AS DATETIME)) + (
        (
          DAYOFWEEK(_s3.min_o_orderdate) + -1
        ) % 7
      ) - (
        (
          DAYOFWEEK(LINEITEM.l_receiptdate) + -1
        ) % 7
      )
    ) / 7 AS SIGNED)
  GROUP BY
    1,
    2
)
SELECT
  ord_wk AS wk,
  n_rows AS n_lines,
  anything_n_rows AS n_orders,
  ROUND(
    SUM(n_rows) OVER (ORDER BY line_wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(anything_n_rows) OVER (ORDER BY ord_wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    4
  ) AS lpo
FROM _t0
ORDER BY
  1
