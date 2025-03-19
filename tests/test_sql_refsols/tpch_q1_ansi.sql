SELECT
  L_RETURNFLAG,
  L_LINESTATUS,
  SUM_QTY,
  SUM_BASE_PRICE,
  SUM_DISC_PRICE,
  SUM_CHARGE,
  AVG_QTY,
  AVG_PRICE,
  AVG_DISC,
  COUNT_ORDER
FROM (
  SELECT
    COALESCE(agg_3, 0) AS COUNT_ORDER,
    COALESCE(agg_4, 0) AS SUM_BASE_PRICE,
    COALESCE(agg_5, 0) AS SUM_CHARGE,
    COALESCE(agg_6, 0) AS SUM_DISC_PRICE,
    COALESCE(agg_7, 0) AS SUM_QTY,
    agg_0 AS AVG_DISC,
    agg_1 AS AVG_PRICE,
    agg_2 AS AVG_QTY,
    return_flag AS L_RETURNFLAG,
    return_flag AS ordering_8,
    status AS L_LINESTATUS,
    status AS ordering_9
  FROM (
    SELECT
      AVG(discount) AS agg_0,
      AVG(extended_price) AS agg_1,
      AVG(quantity) AS agg_2,
      COUNT() AS agg_3,
      SUM(extended_price) AS agg_4,
      SUM(quantity) AS agg_7,
      SUM(extended_price * (
        1 - discount
      )) AS agg_6,
      SUM((
        extended_price * (
          1 - discount
        )
      ) * (
        1 + tax
      )) AS agg_5,
      return_flag,
      status
    FROM (
      SELECT
        discount,
        extended_price,
        quantity,
        return_flag,
        status,
        tax
      FROM (
        SELECT
          l_discount AS discount,
          l_extendedprice AS extended_price,
          l_linestatus AS status,
          l_quantity AS quantity,
          l_returnflag AS return_flag,
          l_shipdate AS ship_date,
          l_tax AS tax
        FROM tpch.LINEITEM
      )
      WHERE
        ship_date <= DATE_STR_TO_DATE('1998-12-01')
    )
    GROUP BY
      status,
      return_flag
  )
)
ORDER BY
  ordering_8,
  ordering_9
