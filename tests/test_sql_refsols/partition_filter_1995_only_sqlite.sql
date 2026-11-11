SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_1995
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1995
