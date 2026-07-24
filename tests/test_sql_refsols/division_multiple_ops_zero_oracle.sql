SELECT
  l_quantity * CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / l_discount END + l_tax AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  l_discount NULLS FIRST,
  l_tax NULLS FIRST
FETCH FIRST 1 ROWS ONLY
