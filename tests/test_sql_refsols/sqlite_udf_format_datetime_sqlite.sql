SELECT
  o_orderkey AS key,
  STRFTIME('%d/%m/%Y', o_orderdate) AS d1,
  STRFTIME('%Y:%j', o_orderdate) AS d2,
  CAST(STRFTIME('%s', o_orderdate) AS INTEGER) AS d3,
  CAST(STRFTIME('%Y%m%d', o_orderdate, '+39 days', 'start of month') AS INTEGER) AS d4
FROM tpch.orders
ORDER BY
  o_totalprice
LIMIT 5
