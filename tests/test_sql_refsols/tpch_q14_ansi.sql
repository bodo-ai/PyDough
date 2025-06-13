WITH _t0 AS (
  SELECT
    SUM(
      CASE
        WHEN part.p_type LIKE 'PROMO%'
        THEN lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        )
        ELSE 0
      END
    ) AS agg_0,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_1
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    EXTRACT(MONTH FROM lineitem.l_shipdate) = 9
    AND EXTRACT(YEAR FROM lineitem.l_shipdate) = 1995
)
SELECT
  (
    100.0 * COALESCE(agg_0, 0)
  ) / COALESCE(agg_1, 0) AS PROMO_REVENUE
FROM _t0
