SELECT
  o_orderdate AS order_date,
  DENSE_RANK() OVER (ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate) AS `rank`
FROM tpch.orders
