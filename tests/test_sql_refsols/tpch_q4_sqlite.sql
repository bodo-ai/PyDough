WITH _u_0 AS (
  SELECT
    l_orderkey AS _u_1
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
  GROUP BY
    l_orderkey
), _t0 AS (
  SELECT
    COUNT(*) AS order_count,
    orders.o_orderpriority AS order_priority
  FROM tpch.orders AS orders
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = orders.o_orderkey
  WHERE
    CASE
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 10
      THEN 4
    END = 3
    AND CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1993
    AND NOT _u_0._u_1 IS NULL
  GROUP BY
    orders.o_orderpriority
)
SELECT
  order_priority AS O_ORDERPRIORITY,
  order_count AS ORDER_COUNT
FROM _t0
ORDER BY
  order_priority
