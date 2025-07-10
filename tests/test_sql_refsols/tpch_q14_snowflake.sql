SELECT
  (
    100.0 * COALESCE(
      SUM(
        IFF(
          STARTSWITH(PART.p_type, 'PROMO'),
          LINEITEM.l_extendedprice * (
            1 - LINEITEM.l_discount
          ),
          0
        )
      ),
      0
    )
  ) / COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS PROMO_REVENUE
FROM TPCH.LINEITEM AS LINEITEM
JOIN TPCH.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey
WHERE
  MONTH(LINEITEM.l_shipdate) = 9 AND YEAR(LINEITEM.l_shipdate) = 1995
