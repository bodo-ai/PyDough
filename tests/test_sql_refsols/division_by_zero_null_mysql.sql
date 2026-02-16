SELECT
  l_extendedprice / NULLIF(l_discount, 0) AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
