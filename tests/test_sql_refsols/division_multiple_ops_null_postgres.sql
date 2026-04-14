SELECT
  l_quantity * (
    CAST(l_extendedprice AS DOUBLE PRECISION) / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
