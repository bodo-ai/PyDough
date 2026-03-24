SELECT
  l_extendedprice / l_discount AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
