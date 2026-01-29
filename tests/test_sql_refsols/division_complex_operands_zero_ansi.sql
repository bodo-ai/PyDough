SELECT
  CASE
    WHEN (
      l_discount * 2
    ) = 0
    THEN 0
    ELSE (
      l_extendedprice + l_tax
    ) / CASE WHEN (
      l_discount * 2
    ) <> 0 THEN l_discount * 2 ELSE NULL END
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
