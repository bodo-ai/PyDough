SELECT
  l_quantity * IF(l_discount = 0, 0, l_extendedprice / l_discount) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount,
  l_tax
LIMIT 1
