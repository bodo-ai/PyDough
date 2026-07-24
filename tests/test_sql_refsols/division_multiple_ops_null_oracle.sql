SELECT
  l_quantity * (
    l_extendedprice / NULLIF(l_discount, 0)
  ) + l_tax AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  l_discount NULLS FIRST,
  l_tax NULLS FIRST
FETCH FIRST 1 ROWS ONLY
