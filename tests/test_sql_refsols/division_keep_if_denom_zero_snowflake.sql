SELECT
  IFF(
    CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END = 0,
    0,
    l_extendedprice / CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1 NULLS FIRST
LIMIT 1
