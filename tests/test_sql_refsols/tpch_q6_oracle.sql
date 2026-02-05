SELECT
  NVL(SUM(l_extendedprice * l_discount), 0) AS REVENUE
FROM TPCH.LINEITEM
WHERE
  l_discount <= 0.07
  AND l_discount >= 0.05
  AND l_quantity < 24
  AND l_shipdate < TO_DATE('1995-01-01', 'YYYY-MM-DD')
  AND l_shipdate >= TO_DATE('1994-01-01', 'YYYY-MM-DD')
