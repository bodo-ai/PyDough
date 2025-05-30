SELECT
  AVG(
    DATEDIFF(
      CASE
        WHEN lineitem.l_commitdate <= lineitem.l_receiptdate
        THEN lineitem.l_commitdate
        WHEN lineitem.l_commitdate >= lineitem.l_receiptdate
        THEN lineitem.l_receiptdate
      END,
      orders.o_orderdate,
      DAY
    )
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
