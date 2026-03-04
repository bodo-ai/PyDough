SELECT
  IF(
    IF(l_discount > 0, 1, l_discount) = 0,
    0,
    CAST(l_extendedprice AS DOUBLE) / IF(l_discount > 0, 1, l_discount)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
