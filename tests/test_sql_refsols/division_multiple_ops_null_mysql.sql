SELECT
  l_quantity * (
    l_extendedprice / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM tpch.LINEITEM
ORDER BY
  l_discount,
  l_tax
LIMIT 1
