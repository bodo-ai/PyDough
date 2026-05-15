SELECT
  l_quantity * (
    CAST(l_extendedprice AS REAL) / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
