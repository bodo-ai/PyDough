SELECT
  l_extendedprice / l_discount AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
