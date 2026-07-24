SELECT
  CAST(l_extendedprice AS DOUBLE) / l_discount AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
