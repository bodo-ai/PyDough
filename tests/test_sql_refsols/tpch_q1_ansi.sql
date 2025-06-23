WITH _t1 AS (
  SELECT
    AVG(l_discount) AS avg_l_discount,
    AVG(l_extendedprice) AS avg_l_extendedprice,
    AVG(l_quantity) AS avg_l_quantity,
    COUNT(*) AS n_rows,
    SUM(l_extendedprice * (
      1 - l_discount
    ) * (
      1 + l_tax
    )) AS sum_expr_8,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_9,
    SUM(l_extendedprice) AS sum_l_extendedprice,
    SUM(l_quantity) AS sum_l_quantity,
    l_linestatus,
    l_returnflag
  FROM tpch.lineitem
  WHERE
    l_shipdate <= CAST('1998-12-01' AS DATE)
  GROUP BY
    l_linestatus,
    l_returnflag
)
SELECT
  l_returnflag AS L_RETURNFLAG,
  l_linestatus AS L_LINESTATUS,
  COALESCE(sum_l_quantity, 0) AS SUM_QTY,
  COALESCE(sum_l_extendedprice, 0) AS SUM_BASE_PRICE,
  COALESCE(sum_expr_9, 0) AS SUM_DISC_PRICE,
  COALESCE(sum_expr_8, 0) AS SUM_CHARGE,
  avg_l_quantity AS AVG_QTY,
  avg_l_extendedprice AS AVG_PRICE,
  avg_l_discount AS AVG_DISC,
  n_rows AS COUNT_ORDER
FROM _t1
ORDER BY
  l_returnflag,
  l_linestatus
