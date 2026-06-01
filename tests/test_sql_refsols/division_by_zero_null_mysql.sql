SELECT
  l_extendedprice / NULLIF(l_discount, 0) AS computed_value
FROM tpch.LINEITEM
ORDER BY
  l_discount
LIMIT 1
