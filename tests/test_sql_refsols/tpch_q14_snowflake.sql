WITH _T0 AS (
  SELECT
    SUM(
      IFF(
        STARTSWITH(PART.p_type, 'PROMO'),
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
    MONTH(LINEITEM.l_shipdate) = 9 AND YEAR(LINEITEM.l_shipdate) = 1995
)
SELECT
  (
    100.0 * COALESCE(
      SUM(
        IFF(
          PART.p_type LIKE 'PROMO%',
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
  DATE_PART(MONTH, CAST(LINEITEM.l_shipdate AS DATETIME)) = 9
  AND DATE_PART(YEAR, CAST(LINEITEM.l_shipdate AS DATETIME)) = 1995
