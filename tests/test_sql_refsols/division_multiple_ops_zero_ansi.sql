SELECT
  l_quantity * CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / NULLIF(l_discount, 0) END + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
