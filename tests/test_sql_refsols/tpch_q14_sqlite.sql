SELECT
  CAST((
    100.0 * COALESCE(
      SUM(
        IIF(
          part.p_type LIKE 'PROMO%',
          lineitem.l_extendedprice * (
            1 - lineitem.l_discount
          ),
          0
        )
      ),
      0
    )
  ) AS REAL) / COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) AS PROMO_REVENUE
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
WHERE
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1995
  AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) = 9
