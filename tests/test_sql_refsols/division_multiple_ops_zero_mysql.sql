SELECT
  l_quantity * CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / l_discount END + l_tax AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
