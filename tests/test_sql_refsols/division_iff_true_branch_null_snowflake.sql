SELECT
  l_extendedprice / NULLIF(IFF(l_discount > 0, l_discount, 1), 0) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
