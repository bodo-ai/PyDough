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
    ) AS agg_0,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_1
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1995
    AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) = 9
)
SELECT
  CAST((
    100.0 * COALESCE(agg_0, 0)
  ) AS REAL) / COALESCE(agg_1, 0) AS PROMO_REVENUE
FROM _t0
