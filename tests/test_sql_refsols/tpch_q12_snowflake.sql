WITH _T1 AS (
  SELECT
    COUNT_IF(ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS AGG_0,
    COUNT_IF(NOT ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS AGG_1,
    LINEITEM.l_shipmode AS SHIP_MODE
  FROM TPCH.LINEITEM AS LINEITEM
  JOIN TPCH.ORDERS AS ORDERS
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey
  WHERE
    LINEITEM.l_commitdate < LINEITEM.l_receiptdate
    AND LINEITEM.l_commitdate > LINEITEM.l_shipdate
    AND (
      LINEITEM.l_shipmode = 'MAIL' OR LINEITEM.l_shipmode = 'SHIP'
    )
    AND YEAR(LINEITEM.l_receiptdate) = 1994
  GROUP BY
    LINEITEM.l_shipmode
)
SELECT
  LINEITEM.l_shipmode AS L_SHIPMODE,
  COALESCE(COUNT_IF(ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS HIGH_LINE_COUNT,
  COALESCE(COUNT_IF(NOT ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS LOW_LINE_COUNT
FROM TPCH.LINEITEM AS LINEITEM
JOIN TPCH.ORDERS AS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
WHERE
  DATE_PART(YEAR, CAST(LINEITEM.l_receiptdate AS DATETIME)) = 1994
  AND LINEITEM.l_commitdate < LINEITEM.l_receiptdate
  AND LINEITEM.l_commitdate > LINEITEM.l_shipdate
  AND (
    LINEITEM.l_shipmode = 'MAIL' OR LINEITEM.l_shipmode = 'SHIP'
  )
GROUP BY
  LINEITEM.l_shipmode
ORDER BY
  L_SHIPMODE NULLS FIRST
