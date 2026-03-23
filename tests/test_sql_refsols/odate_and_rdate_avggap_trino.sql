SELECT
  AVG(
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', orders.o_orderdate) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', LEAST(lineitem.l_commitdate, lineitem.l_receiptdate)) AS TIMESTAMP)
    )
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
