SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_a,
  COALESCE(SUM(o_totalprice), 0) AS total_b
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1995
