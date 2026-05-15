SELECT
  AVG(
    DATEDIFF(
      DAY,
      CAST(orders.o_orderdate AS DATETIME),
      CAST(LEAST(lineitem.l_commitdate, lineitem.l_receiptdate) AS DATETIME)
    )
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
