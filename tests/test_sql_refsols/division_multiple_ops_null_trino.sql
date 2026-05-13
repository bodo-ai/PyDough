SELECT
  l_quantity * (
    CAST(l_extendedprice AS DOUBLE) / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST,
  l_tax NULLS FIRST
LIMIT 1
