SELECT
  IIF(
    IIF(l_discount > 0, l_discount, 1) = 0,
    0,
    CAST(l_extendedprice AS REAL) / IIF(l_discount > 0, l_discount, 1)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
