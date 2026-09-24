WITH "_t3" AS (
  SELECT
    O_ORDERDATE
  FROM TPCH.ORDERS
), "_s0" AS (
  SELECT
    MIN(O_ORDERDATE) AS MIN_O_ORDERDATE
  FROM "_t3"
), "_s2" AS (
  SELECT
    FLOOR(
      (
        TRUNC(CAST(CAST(ORDERS.O_ORDERDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s0".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s0".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(ORDERS.O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    ) AS ORD_WK,
    COUNT(*) AS N_ROWS
  FROM "_s0" "_s0"
  JOIN TPCH.ORDERS ORDERS
    ON FLOOR(
      (
        TRUNC(CAST(CAST(ORDERS.O_ORDERDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s0".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s0".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(ORDERS.O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    ) < 10
    AND ORDERS.O_ORDERPRIORITY = '1-URGENT'
    AND ORDERS.O_ORDERSTATUS = 'F'
  GROUP BY
    FLOOR(
      (
        TRUNC(CAST(CAST(ORDERS.O_ORDERDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s0".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s0".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(ORDERS.O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    )
), "_s3" AS (
  SELECT
    MIN(O_ORDERDATE) AS MIN_O_ORDERDATE
  FROM "_t3"
), "_t0" AS (
  SELECT
    FLOOR(
      (
        TRUNC(CAST(CAST(LINEITEM.L_RECEIPTDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s3".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s3".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(LINEITEM.L_RECEIPTDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    ) AS LINE_WK,
    "_s2".ORD_WK,
    ANY_VALUE("_s2".N_ROWS) AS ANYTHING_N_ROWS,
    COUNT(*) AS N_ROWS
  FROM "_s2" "_s2"
  CROSS JOIN "_s3" "_s3"
  JOIN TPCH.LINEITEM LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.L_RECEIPTDATE AS DATE)) = 1992
    AND FLOOR(
      (
        TRUNC(CAST(CAST(LINEITEM.L_RECEIPTDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s3".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s3".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(LINEITEM.L_RECEIPTDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    ) < 10
    AND LINEITEM.L_RETURNFLAG = 'R'
    AND LINEITEM.L_SHIPMODE = 'RAIL'
    AND "_s2".ORD_WK = FLOOR(
      (
        TRUNC(CAST(CAST(LINEITEM.L_RECEIPTDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s3".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s3".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(LINEITEM.L_RECEIPTDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    )
  GROUP BY
    FLOOR(
      (
        TRUNC(CAST(CAST(LINEITEM.L_RECEIPTDATE AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST("_s3".MIN_O_ORDERDATE AS DATE) AS DATE), 'DD') + (
          MOD((
            TO_CHAR(CAST("_s3".MIN_O_ORDERDATE AS DATE), 'D') + -1
          ), 7)
        ) - (
          MOD((
            TO_CHAR(CAST(LINEITEM.L_RECEIPTDATE AS DATE), 'D') + -1
          ), 7)
        )
      ) / 7
    ),
    "_s2".ORD_WK
)
SELECT
  ORD_WK AS wk,
  N_ROWS AS n_lines,
  ANYTHING_N_ROWS AS n_orders,
  ROUND(
    SUM(N_ROWS) OVER (ORDER BY LINE_WK ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(ANYTHING_N_ROWS) OVER (ORDER BY ORD_WK ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    4
  ) AS lpo
FROM "_t0"
ORDER BY
  1 NULLS FIRST
