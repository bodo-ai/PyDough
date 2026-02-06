SELECT
  IFF(l_discount = 0, 0, l_extendedprice / NULLIF(l_discount, 0)) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
