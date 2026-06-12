SELECT
  l_extendedprice / l_discount AS computed_value
FROM tpch.LINEITEM
ORDER BY
  l_discount
LIMIT 1
