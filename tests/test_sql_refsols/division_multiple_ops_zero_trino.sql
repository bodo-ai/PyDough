SELECT
  l_quantity * IF(l_discount = 0, 0, CAST(l_extendedprice AS DOUBLE) / l_discount) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST,
  l_tax NULLS FIRST
LIMIT 1
