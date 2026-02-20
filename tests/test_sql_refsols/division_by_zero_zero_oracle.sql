SELECT
  CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / l_discount END AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
