SELECT
  IIF(
    IIF(l_discount > 0, 1, l_discount) = 0,
    0,
    CAST(l_extendedprice AS REAL) / IIF(l_discount > 0, 1, l_discount)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
