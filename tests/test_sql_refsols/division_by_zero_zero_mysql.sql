SELECT
  CASE WHEN l_discount = 0 THEN 0 ELSE l_extendedprice / l_discount END AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
