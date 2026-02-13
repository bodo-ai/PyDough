SELECT
  l_quantity * IIF(l_discount = 0, 0, CAST(l_extendedprice AS REAL) / l_discount) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
