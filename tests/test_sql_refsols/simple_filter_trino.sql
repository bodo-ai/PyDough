SELECT
  o_orderdate AS order_date,
  o_orderkey,
  o_totalprice
FROM mysql.tpch.orders
WHERE
  o_totalprice < 1000.0
