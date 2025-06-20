WITH _T0 AS (
  SELECT
    SUM(l_extendedprice * l_discount) AS AGG_0
  FROM TPCH.LINEITEM
  WHERE
    l_discount <= 0.07
    AND l_discount >= 0.05
    AND l_quantity < 24
    AND l_shipdate < CAST('1995-01-01' AS DATE)
    AND l_shipdate >= CAST('1994-01-01' AS DATE)
)
SELECT
  COALESCE(AGG_0, 0) AS REVENUE
FROM _T0
