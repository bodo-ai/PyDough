SELECT
  AVG(
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', CAST(orders.o_orderdate AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', CAST(LEAST(lineitem.l_commitdate, lineitem.l_receiptdate) AS TIMESTAMP)) AS TIMESTAMP)
    )
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
