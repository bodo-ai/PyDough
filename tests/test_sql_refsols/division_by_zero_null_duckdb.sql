SELECT
  l_extendedprice / NULLIF(l_discount, 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
