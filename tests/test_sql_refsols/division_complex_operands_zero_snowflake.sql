SELECT
  IFF(
    (
      l_discount * 2
    ) = 0,
    0,
    (
      l_extendedprice + l_tax
    ) / CASE WHEN (
      l_discount * 2
    ) <> 0 THEN l_discount * 2 ELSE NULL END
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
