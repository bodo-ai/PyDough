SELECT
  CAST(l_extendedprice AS DOUBLE PRECISION) / l_discount AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
