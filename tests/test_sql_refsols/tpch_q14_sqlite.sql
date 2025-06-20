WITH _t0 AS (
  SELECT
    SUM(IIF(_s1.p_type LIKE 'PROMO%', _s0.l_extendedprice * (
      1 - _s0.l_discount
    ), 0)) AS agg_0,
    SUM(_s0.l_extendedprice * (
      1 - _s0.l_discount
    )) AS agg_1
  FROM tpch.lineitem AS _s0
  JOIN tpch.part AS _s1
    ON _s0.l_partkey = _s1.p_partkey
  WHERE
    CAST(STRFTIME('%Y', _s0.l_shipdate) AS INTEGER) = 1995
    AND CAST(STRFTIME('%m', _s0.l_shipdate) AS INTEGER) = 9
)
SELECT
  CAST((
    100.0 * COALESCE(agg_0, 0)
  ) AS REAL) / COALESCE(agg_1, 0) AS PROMO_REVENUE
FROM _t0
