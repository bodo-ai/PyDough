SELECT
  CAST(l_extendedprice AS DOUBLE PRECISION) / NULLIF(CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END, 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
