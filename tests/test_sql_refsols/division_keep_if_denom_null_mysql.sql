SELECT
  l_extendedprice / NULLIF(CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END, 0) AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
