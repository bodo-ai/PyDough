SELECT
  l_extendedprice / NULLIF(IF(l_discount > 0, 1, l_discount), 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
