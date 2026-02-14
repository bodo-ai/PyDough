SELECT
  AVG(
    DATEDIFF(
      CAST(LEAST(LINEITEM.l_commitdate, LINEITEM.l_receiptdate) AS DATETIME),
      CAST(ORDERS.o_orderdate AS DATETIME),
      DAY
    )
  ) AS avg_gap
FROM TPCH.LINEITEM LINEITEM
JOIN TPCH.ORDERS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
WHERE
  LINEITEM.l_shipmode = 'RAIL'
