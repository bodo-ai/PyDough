SELECT
  AVG(
    TRUNC(
      CAST(CAST(LEAST(LINEITEM.l_commitdate, LINEITEM.l_receiptdate) AS DATE) AS DATE),
      'DD'
    ) - TRUNC(CAST(CAST(ORDERS.o_orderdate AS DATE) AS DATE), 'DD')
  ) AS avg_gap
FROM TPCH.LINEITEM LINEITEM
JOIN TPCH.ORDERS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
WHERE
  LINEITEM.l_shipmode = 'RAIL'
