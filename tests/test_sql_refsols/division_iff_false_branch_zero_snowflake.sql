SELECT
  IFF(
    IFF(l_discount > 0, 1, l_discount) = 0,
    0,
    l_extendedprice / IFF(l_discount > 0, 1, l_discount)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount NULLS FIRST
LIMIT 1
