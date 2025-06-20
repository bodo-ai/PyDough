WITH _T0 AS (
  SELECT
    SUM(
      IFF(
        PART.p_type LIKE 'PROMO%',
        LINEITEM.l_extendedprice * (
          1 - LINEITEM.l_discount
        ),
        0
      )
    ) AS AGG_0,
    SUM(LINEITEM.l_extendedprice * (
      1 - LINEITEM.l_discount
    )) AS AGG_1
  FROM TPCH.LINEITEM AS LINEITEM
  JOIN TPCH.PART AS PART
    ON LINEITEM.l_partkey = PART.p_partkey
  WHERE
    DATE_PART(MONTH, LINEITEM.l_shipdate) = 9
    AND DATE_PART(YEAR, LINEITEM.l_shipdate) = 1995
)
SELECT
  (
    100.0 * COALESCE(AGG_0, 0)
  ) / COALESCE(AGG_1, 0) AS PROMO_REVENUE
FROM _T0
