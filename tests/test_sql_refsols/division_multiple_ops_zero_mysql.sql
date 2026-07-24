SELECT
  l_quantity * CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / l_discount END + l_tax AS computed_value
FROM tpch.LINEITEM
ORDER BY
  l_discount,
  l_tax
LIMIT 1
