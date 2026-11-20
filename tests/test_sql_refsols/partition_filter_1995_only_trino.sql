SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_1995
FROM mysql.tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
