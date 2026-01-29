SELECT
  CASE
    WHEN (
      l_discount * 2
    ) = 0
    THEN 0
    ELSE CAST((
      l_extendedprice + l_tax
    ) AS DOUBLE PRECISION) / CASE WHEN (
      l_discount * 2
    ) <> 0 THEN l_discount * 2 ELSE NULL END
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
