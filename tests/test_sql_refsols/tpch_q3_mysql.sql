SELECT
  LINEITEM.l_orderkey AS L_ORDERKEY,
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS REVENUE,
  ORDERS.o_orderdate AS O_ORDERDATE,
  ORDERS.o_shippriority AS O_SHIPPRIORITY
FROM tpch.ORDERS AS ORDERS
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = ORDERS.o_custkey AND CUSTOMER.c_mktsegment = 'BUILDING'
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
  AND LINEITEM.l_shipdate > CAST('1995-03-15' AS DATE)
WHERE
  ORDERS.o_orderdate < CAST('1995-03-15' AS DATE)
GROUP BY
  1,
  3,
  4
ORDER BY
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) DESC,
  ORDERS.o_orderdate,
  LINEITEM.l_orderkey
LIMIT 10
