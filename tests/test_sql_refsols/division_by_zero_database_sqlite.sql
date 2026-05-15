SELECT
  CAST(l_extendedprice AS REAL) / l_discount AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
