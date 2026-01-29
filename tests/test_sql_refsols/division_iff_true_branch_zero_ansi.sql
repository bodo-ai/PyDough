SELECT
  CASE
    WHEN CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END = 0
    THEN 0
    ELSE l_extendedprice / NULLIF(CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END, 0)
  END AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
