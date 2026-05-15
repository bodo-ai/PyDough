SELECT
  l_quantity * (
    l_extendedprice / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
