SELECT
  l_extendedprice / NULLIF(CASE WHEN l_discount > 0 THEN 1 ELSE l_discount END, 0) AS computed_value
FROM tpch.LINEITEM
ORDER BY
  l_discount
LIMIT 1
