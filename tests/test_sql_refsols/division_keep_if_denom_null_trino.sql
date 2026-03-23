SELECT
  CAST(l_extendedprice AS DOUBLE) / NULLIF(CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END, 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
