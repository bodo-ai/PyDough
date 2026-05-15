SELECT
  CASE
    WHEN CASE WHEN l_discount > 0 THEN 1 ELSE l_discount END = 0
    THEN 0
    ELSE CAST(l_extendedprice AS DOUBLE PRECISION) / CASE WHEN l_discount > 0 THEN 1 ELSE l_discount END
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
