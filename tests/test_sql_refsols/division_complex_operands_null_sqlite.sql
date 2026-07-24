SELECT
  CAST((
    l_extendedprice + l_tax
  ) AS REAL) / CASE WHEN (
    l_discount * 2
  ) <> 0 THEN l_discount * 2 ELSE NULL END AS computed_value
FROM tpch.lineitem
ORDER BY
  l_discount
LIMIT 1
