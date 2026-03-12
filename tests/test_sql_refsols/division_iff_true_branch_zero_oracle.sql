SELECT
  CASE
    WHEN CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END = 0
    THEN 0
    ELSE l_extendedprice / CASE WHEN l_discount > 0 THEN l_discount ELSE 1 END
  END AS computed_value
FROM TPCH.LINEITEM
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
