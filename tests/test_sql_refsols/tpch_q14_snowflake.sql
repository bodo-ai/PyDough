SELECT
  (
    100.0 * COALESCE(
      SUM(
        IFF(
          STARTSWITH(part.p_type, 'PROMO'),
          lineitem.l_extendedprice * (
            1 - lineitem.l_discount
          ),
          0
        )
      ),
      0
    )
  ) / COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) AS PROMO_REVENUE
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
WHERE
  MONTH(CAST(lineitem.l_shipdate AS TIMESTAMP)) = 9
  AND YEAR(CAST(lineitem.l_shipdate AS TIMESTAMP)) = 1995
