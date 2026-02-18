SELECT
  l_extendedprice / NULLIF(CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END, 0) AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
