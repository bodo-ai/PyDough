SELECT
  o_orderdate AS order_date,
  DENSE_RANK() OVER (ORDER BY o_orderdate) AS rank
FROM TPCH.ORDERS
