WITH _t1 AS (
  SELECT
    AVG(l_discount) AS agg_0,
    AVG(l_extendedprice) AS agg_1,
    AVG(l_quantity) AS agg_2,
    COUNT() AS agg_3,
    SUM(l_extendedprice) AS agg_4,
    SUM(l_extendedprice * (
      1 - l_discount
    ) * (
      1 + l_tax
    )) AS agg_5,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_6,
    SUM(l_quantity) AS agg_7,
    l_returnflag AS return_flag,
    l_linestatus AS status
  FROM tpch.lineitem
  WHERE
    l_shipdate <= CAST('1998-12-01' AS DATE)
  GROUP BY
    l_linestatus,
    l_returnflag
)
SELECT
  return_flag AS L_RETURNFLAG,
  status AS L_LINESTATUS,
  COALESCE(agg_7, 0) AS SUM_QTY,
  COALESCE(agg_4, 0) AS SUM_BASE_PRICE,
  COALESCE(agg_6, 0) AS SUM_DISC_PRICE,
  COALESCE(agg_5, 0) AS SUM_CHARGE,
  agg_2 AS AVG_QTY,
  agg_1 AS AVG_PRICE,
  agg_0 AS AVG_DISC,
  agg_3 AS COUNT_ORDER
FROM _t1
ORDER BY
  return_flag,
  status
