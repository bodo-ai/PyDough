SELECT
  COALESCE(SUM(l_extendedprice * l_discount), 0) AS REVENUE
FROM tpch.lineitem
WHERE
  l_discount <= 0.07
  AND l_discount >= 0.05
  AND l_quantity < 24
  AND l_shipdate < CAST('1995-01-01' AS DATE)
  AND l_shipdate >= CAST('1994-01-01' AS DATE)
