SELECT
  IFF(
    IFF(l_discount > 0, l_discount, 1) = 0,
    0,
    l_extendedprice / IFF(l_discount > 0, l_discount, 1)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
