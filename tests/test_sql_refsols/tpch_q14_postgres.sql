SELECT
  (
    100.0 * COALESCE(
      SUM(
        CASE
          WHEN part.p_type LIKE 'PROMO%'
          THEN lineitem.l_extendedprice * (
            1 - lineitem.l_discount
          )
          ELSE 0
        END
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
  EXTRACT(MONTH FROM CAST(lineitem.l_shipdate AS TIMESTAMP)) = 9
  AND EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS TIMESTAMP)) = 1995
