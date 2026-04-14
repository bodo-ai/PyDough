SELECT
  AVG(
    CAST((
      JULIANDAY(DATE(MIN(lineitem.l_commitdate, lineitem.l_receiptdate), 'start of day')) - JULIANDAY(DATE(orders.o_orderdate, 'start of day'))
    ) AS INTEGER)
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
