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
    ) AS sum_promo_value,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_value
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    EXTRACT(MONTH FROM CAST(lineitem.l_shipdate AS DATETIME)) = 9
    AND EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1995
)
SELECT
  (
    100.0 * COALESCE(sum_promo_value, 0)
  ) / COALESCE(sum_value, 0) AS PROMO_REVENUE
FROM _t0
