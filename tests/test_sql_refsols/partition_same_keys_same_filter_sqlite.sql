SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_a,
  COALESCE(SUM(o_totalprice), 0) AS total_b
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1995
