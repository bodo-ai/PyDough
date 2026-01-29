SELECT
  IIF(
    CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END = 0,
    0,
    CAST(l_extendedprice AS REAL) / NULLIF(CASE WHEN l_discount > 0.05 THEN l_discount ELSE NULL END, 0)
  ) AS computed_value
FROM tpch.lineitem
ORDER BY
  1
LIMIT 1
