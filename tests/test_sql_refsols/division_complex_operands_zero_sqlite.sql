SELECT
  IIF(
    (
      l_discount * 2
    ) = 0,
    0,
    CAST((
      l_extendedprice + l_tax
    ) AS REAL) / (
      l_discount * 2
    )
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
