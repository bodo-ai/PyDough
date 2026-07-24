SELECT
  CAST(l_extendedprice AS REAL) / NULLIF(IIF(l_discount > 0, l_discount, 1), 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
