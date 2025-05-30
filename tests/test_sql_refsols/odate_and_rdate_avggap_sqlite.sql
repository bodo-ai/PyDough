WITH _t0 AS (
  SELECT
    CAST((
      JULIANDAY(DATE(MIN(lineitem.l_commitdate, lineitem.l_receiptdate), 'start of day')) - JULIANDAY(DATE(orders.o_orderdate, 'start of day'))
    ) AS INTEGER) AS day_gap
  FROM tpch.lineitem AS lineitem
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    lineitem.l_shipmode = 'RAIL'
)
SELECT
  AVG(day_gap) AS avg_gap
FROM _t0
