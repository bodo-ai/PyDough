SELECT
  l_returnflag AS L_RETURNFLAG,
  l_linestatus AS L_LINESTATUS,
  COALESCE(SUM(l_quantity), 0) AS SUM_QTY,
  COALESCE(SUM(l_extendedprice), 0) AS SUM_BASE_PRICE,
  COALESCE(SUM(l_extendedprice * (
    1 - l_discount
  )), 0) AS SUM_DISC_PRICE,
  COALESCE(SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )), 0) AS SUM_CHARGE,
  AVG(l_quantity) AS AVG_QTY,
  AVG(l_extendedprice) AS AVG_PRICE,
  AVG(l_discount) AS AVG_DISC,
  COUNT(*) AS COUNT_ORDER
FROM TPCH.LINEITEM
WHERE
  l_shipdate <= TO_DATE('1998-12-01', 'YYYY-MM-DD')
GROUP BY
  l_linestatus,
  l_returnflag
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
