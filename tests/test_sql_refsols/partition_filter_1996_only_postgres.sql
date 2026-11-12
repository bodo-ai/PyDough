SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_1996
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996
