SELECT
  IF(
    IF(l_discount > 0, l_discount, 1) = 0,
    0,
    l_extendedprice / IF(l_discount > 0, l_discount, 1)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
