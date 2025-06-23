WITH _t0 AS (
  SELECT
    SUM(
      IIF(
        part.p_type LIKE 'PROMO%',
        lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        ),
        0
      )
    ) AS sum_promo_value,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_value
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1995
    AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) = 9
)
SELECT
  CAST((
    100.0 * COALESCE(sum_promo_value, 0)
  ) AS REAL) / COALESCE(sum_value, 0) AS PROMO_REVENUE
FROM _t0
