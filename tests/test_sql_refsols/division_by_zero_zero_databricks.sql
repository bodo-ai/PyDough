SELECT
  IF(l_discount = 0, 0, l_extendedprice / l_discount) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
