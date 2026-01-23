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
    DATEDIFF(CAST(orders.o_orderdate AS DATETIME), CAST(_s0.min_o_orderdate AS DATETIME), WEEK) AS ord_wk,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN tpch.orders AS orders
    ON DATEDIFF(CAST(orders.o_orderdate AS DATETIME), CAST(_s0.min_o_orderdate AS DATETIME), WEEK) < 10
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
    DATEDIFF(
      CAST(lineitem.l_receiptdate AS DATETIME),
      CAST(_s3.min_o_orderdate AS DATETIME),
      WEEK
    ) AS line_wk,
    _s2.ord_wk,
    ANY_VALUE(_s2.n_rows) AS anything_n_rows,
    COUNT(*) AS n_rows
  FROM _s2 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN tpch.lineitem AS lineitem
    ON DATEDIFF(
      CAST(lineitem.l_receiptdate AS DATETIME),
      CAST(_s3.min_o_orderdate AS DATETIME),
      WEEK
    ) < 10
    AND EXTRACT(YEAR FROM CAST(lineitem.l_receiptdate AS DATETIME)) = 1992
    AND _s2.ord_wk = DATEDIFF(
      CAST(lineitem.l_receiptdate AS DATETIME),
      CAST(_s3.min_o_orderdate AS DATETIME),
      WEEK
    )
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
    SUM(n_rows) OVER (ORDER BY line_wk NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(anything_n_rows) OVER (ORDER BY ord_wk NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    4
  ) AS lpo
FROM _t0
ORDER BY
  1
