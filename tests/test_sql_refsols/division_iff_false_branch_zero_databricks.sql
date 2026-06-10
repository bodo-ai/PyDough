SELECT
  IF(
    IF(l_discount > 0, 1, l_discount) = 0,
    0,
    l_extendedprice / IF(l_discount > 0, 1, l_discount)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
