WITH _t0 AS (
  SELECT
    COALESCE(SUM(l_extendedprice), 0) AS sum_base_price,
    COALESCE(SUM(l_extendedprice * (
      1 - l_discount
    ) * (
      1 + l_tax
    )), 0) AS sum_charge,
    COALESCE(SUM(l_extendedprice * (
      1 - l_discount
    )), 0) AS sum_disc_price,
    COALESCE(SUM(l_quantity), 0) AS sum_qty,
    AVG(l_discount) AS avg_l_discount,
    AVG(l_extendedprice) AS avg_l_extendedprice,
    AVG(l_quantity) AS avg_l_quantity,
    l_linestatus,
    l_returnflag,
    COUNT(*) AS n_rows
  FROM tpch.lineitem
  WHERE
    l_shipdate <= '1998-12-01'
  GROUP BY
    l_linestatus,
    l_returnflag
)
SELECT
  l_returnflag AS L_RETURNFLAG,
  l_linestatus AS L_LINESTATUS,
  sum_qty AS SUM_QTY,
  sum_base_price AS SUM_BASE_PRICE,
  sum_disc_price AS SUM_DISC_PRICE,
  sum_charge AS SUM_CHARGE,
  avg_l_quantity AS AVG_QTY,
  avg_l_extendedprice AS AVG_PRICE,
  avg_l_discount AS AVG_DISC,
  n_rows AS COUNT_ORDER
FROM _t0
ORDER BY
  l_returnflag,
  l_linestatus
