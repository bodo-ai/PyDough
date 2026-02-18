SELECT
  CASE
    WHEN CASE WHEN l_discount > 0 THEN 1 ELSE l_discount END = 0
    THEN 0
    ELSE l_extendedprice / CASE WHEN l_discount > 0 THEN 1 ELSE l_discount END
  END AS computed_value
FROM tpch.LINEITEM
ORDER BY
  1
LIMIT 1
