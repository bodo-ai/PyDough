SELECT
  AVG(
    DATE_DIFF(
      'DAY',
      CAST(orders.o_orderdate AS TIMESTAMP),
      CAST(CASE
        WHEN lineitem.l_commitdate <= lineitem.l_receiptdate
        THEN lineitem.l_commitdate
        WHEN lineitem.l_commitdate >= lineitem.l_receiptdate
        THEN lineitem.l_receiptdate
      END AS TIMESTAMP)
    )
  ) AS avg_gap
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
