SELECT
  CASE
    WHEN (
      l_discount * 2
    ) = 0
    THEN 0
    ELSE (
      l_extendedprice + l_tax
    ) / (
      l_discount * 2
    )
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
