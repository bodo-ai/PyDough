WITH _T1 AS (
  SELECT
    AVG(l_discount) AS AGG_0,
    AVG(l_extendedprice) AS AGG_1,
    AVG(l_quantity) AS AGG_2,
    COUNT(*) AS AGG_3,
    SUM(l_extendedprice) AS AGG_4,
    SUM(l_extendedprice * (
      1 - l_discount
    ) * (
      1 + l_tax
    )) AS AGG_5,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS AGG_6,
    SUM(l_quantity) AS AGG_7,
    l_returnflag AS RETURN_FLAG,
    l_linestatus AS STATUS
  FROM TPCH.LINEITEM
  WHERE
    l_shipdate <= CAST('1998-12-01' AS DATE)
  GROUP BY
    l_returnflag,
    l_linestatus
)
SELECT
  RETURN_FLAG AS L_RETURNFLAG,
  STATUS AS L_LINESTATUS,
  COALESCE(AGG_7, 0) AS SUM_QTY,
  COALESCE(AGG_4, 0) AS SUM_BASE_PRICE,
  COALESCE(AGG_6, 0) AS SUM_DISC_PRICE,
  COALESCE(AGG_5, 0) AS SUM_CHARGE,
  AGG_2 AS AVG_QTY,
  AGG_1 AS AVG_PRICE,
  AGG_0 AS AVG_DISC,
  AGG_3 AS COUNT_ORDER
FROM _T1
ORDER BY
  L_RETURNFLAG NULLS FIRST,
  L_LINESTATUS NULLS FIRST
