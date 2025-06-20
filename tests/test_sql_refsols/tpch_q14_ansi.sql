WITH _t0 AS (
  SELECT
    SUM(
      CASE
        WHEN _s1.p_type LIKE 'PROMO%'
        THEN _s0.l_extendedprice * (
          1 - _s0.l_discount
        )
        ELSE 0
      END
    ) AS agg_0,
    SUM(_s0.l_extendedprice * (
      1 - _s0.l_discount
    )) AS agg_1
  FROM tpch.lineitem AS _s0
  JOIN tpch.part AS _s1
    ON _s0.l_partkey = _s1.p_partkey
  WHERE
    EXTRACT(MONTH FROM _s0.l_shipdate) = 9
    AND EXTRACT(YEAR FROM _s0.l_shipdate) = 1995
)
SELECT
  (
    100.0 * COALESCE(agg_0, 0)
  ) / COALESCE(agg_1, 0) AS PROMO_REVENUE
FROM _t0
