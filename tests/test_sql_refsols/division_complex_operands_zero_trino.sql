SELECT
  IF(
    (
      l_discount * 2
    ) = 0,
    0,
    CAST((
      l_extendedprice + l_tax
    ) AS DOUBLE) / (
      l_discount * 2
    )
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
