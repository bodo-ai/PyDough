WITH "_T3" AS (
  SELECT
    o_orderdate AS O_ORDERDATE
  FROM TPCH.ORDERS
), "_S0" AS (
  SELECT
    MIN(O_ORDERDATE) AS MIN_O_ORDERDATE
  FROM "_T3"
), "_S2" AS (
  SELECT
    FLOOR(
      (
        CAST(ORDERS.o_orderdate AS DATE) - CAST("_S0".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S0".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(ORDERS.o_orderdate, 'D') + -1
          ), 7)
        )
      ) / 7
    ) AS ORD_WK,
    COUNT(*) AS N_ROWS
  FROM "_S0" "_S0"
  JOIN TPCH.ORDERS ORDERS
    ON FLOOR(
      (
        CAST(ORDERS.o_orderdate AS DATE) - CAST("_S0".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S0".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(ORDERS.o_orderdate, 'D') + -1
          ), 7)
        )
      ) / 7
    ) < 10
    AND ORDERS.o_orderpriority = '1-URGENT'
    AND ORDERS.o_orderstatus = 'F'
  GROUP BY
    FLOOR(
      (
        CAST(ORDERS.o_orderdate AS DATE) - CAST("_S0".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S0".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(ORDERS.o_orderdate, 'D') + -1
          ), 7)
        )
      ) / 7
    )
), "_S3" AS (
  SELECT
    MIN(O_ORDERDATE) AS MIN_O_ORDERDATE
  FROM "_T3"
), "_T0" AS (
  SELECT
    FLOOR(
      (
        CAST(LINEITEM.l_receiptdate AS DATE) - CAST("_S3".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S3".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(LINEITEM.l_receiptdate, 'D') + -1
          ), 7)
        )
      ) / 7
    ) AS LINE_WK,
    "_S2".ORD_WK,
    ANY_VALUE("_S2".N_ROWS) AS ANYTHING_N_ROWS,
    COUNT(*) AS N_ROWS
  FROM "_S2" "_S2"
  CROSS JOIN "_S3" "_S3"
  JOIN TPCH.LINEITEM LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_receiptdate AS DATE)) = 1992
    AND FLOOR(
      (
        CAST(LINEITEM.l_receiptdate AS DATE) - CAST("_S3".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S3".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(LINEITEM.l_receiptdate, 'D') + -1
          ), 7)
        )
      ) / 7
    ) < 10
    AND LINEITEM.l_returnflag = 'R'
    AND LINEITEM.l_shipmode = 'RAIL'
    AND "_S2".ORD_WK = FLOOR(
      (
        CAST(LINEITEM.l_receiptdate AS DATE) - CAST("_S3".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S3".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(LINEITEM.l_receiptdate, 'D') + -1
          ), 7)
        )
      ) / 7
    )
  GROUP BY
    FLOOR(
      (
        CAST(LINEITEM.l_receiptdate AS DATE) - CAST("_S3".MIN_O_ORDERDATE AS DATE) + (
          MOD((
            TO_CHAR("_S3".MIN_O_ORDERDATE, 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(LINEITEM.l_receiptdate, 'D') + -1
          ), 7)
        )
      ) / 7
    ),
    "_S2".ORD_WK
)
SELECT
  ORD_WK AS wk,
  N_ROWS AS n_lines,
  ANYTHING_N_ROWS AS n_orders,
  ROUND(
    SUM(N_ROWS) OVER (ORDER BY LINE_WK ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(ANYTHING_N_ROWS) OVER (ORDER BY ORD_WK ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    4
  ) AS lpo
FROM "_T0"
ORDER BY
  1 NULLS FIRST
