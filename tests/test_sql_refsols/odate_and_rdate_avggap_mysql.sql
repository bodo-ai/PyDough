SELECT
  AVG(
    DATEDIFF(LEAST(LINEITEM.l_commitdate, LINEITEM.l_receiptdate), ORDERS.o_orderdate)
  ) AS avg_gap
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.ORDERS AS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
WHERE
  LINEITEM.l_shipmode = 'RAIL'
