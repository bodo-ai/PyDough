WITH _t0_2 AS (
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
  LEFT JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    lineitem.l_shipdate < '1995-10-01' AND lineitem.l_shipdate >= '1995-09-01'
)
SELECT
  CAST((
    100.0 * COALESCE(_t0.agg_0, 0)
  ) AS REAL) / COALESCE(_t0.agg_1, 0) AS PROMO_REVENUE
FROM _t0_2 AS _t0
