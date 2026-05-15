SELECT
  CAST(l_extendedprice AS DOUBLE PRECISION) / NULLIF(l_discount, 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
