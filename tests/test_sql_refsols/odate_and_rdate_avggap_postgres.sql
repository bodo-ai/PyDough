SELECT
  AVG(
    CAST(CAST(LEAST(lineitem.l_commitdate, lineitem.l_receiptdate) AS DATE) - CAST(orders.o_orderdate AS DATE) AS DECIMAL)
  ) AS avg_gap
FROM main.lineitem AS lineitem
JOIN main.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
