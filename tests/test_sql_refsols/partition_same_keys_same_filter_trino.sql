SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_a,
  COALESCE(SUM(o_totalprice), 0) AS total_b
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
