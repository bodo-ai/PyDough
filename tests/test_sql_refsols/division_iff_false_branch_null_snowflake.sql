SELECT
  l_extendedprice / NULLIF(IFF(l_discount > 0, 1, l_discount), 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
