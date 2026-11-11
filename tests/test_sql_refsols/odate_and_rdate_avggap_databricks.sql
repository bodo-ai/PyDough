SELECT
  AVG(
    DATEDIFF(
      DAY,
      CAST(orders.o_orderdate AS DATE),
      CAST(LEAST(lineitem.l_commitdate, lineitem.l_receiptdate) AS DATE)
    )
  ) AS avg_gap
FROM tpch_s3_pq.tpch_sf1.lineitem AS lineitem
JOIN tpch_s3_pq.tpch_sf1.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  lineitem.l_shipmode = 'RAIL'
