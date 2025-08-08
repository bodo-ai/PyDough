SELECT
  (
    100.0 * COALESCE(
      SUM(
        CASE
          WHEN PART.p_type LIKE 'PROMO%'
          THEN LINEITEM.l_extendedprice * (
            1 - LINEITEM.l_discount
          )
          ELSE 0
        END
      ),
      0
    )
  ) / COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS PROMO_REVENUE
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey
WHERE
  EXTRACT(MONTH FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 9
  AND EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1995
