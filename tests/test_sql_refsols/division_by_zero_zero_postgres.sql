SELECT
  CASE
    WHEN l_discount = 0
    THEN 0
    ELSE CAST(l_extendedprice AS DOUBLE PRECISION) / l_discount
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
