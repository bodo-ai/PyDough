WITH _s6 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s1.merchant_id
  FROM main.coupons AS _s1
  JOIN main.merchants AS _s2
    ON (
      (
        CAST(STRFTIME('%Y', _s1.created_at) AS INTEGER) - CAST(STRFTIME('%Y', _s2.created_at) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', _s1.created_at) AS INTEGER) - CAST(STRFTIME('%m', _s2.created_at) AS INTEGER)
    ) = 0
    AND _s1.merchant_id = _s2.mid
  GROUP BY
    _s1.merchant_id
)
SELECT
  _s0.mid AS merchant_id,
  _s0.name AS merchant_name,
  COALESCE(_s6.agg_0, 0) AS coupons_per_merchant
FROM main.merchants AS _s0
LEFT JOIN _s6 AS _s6
  ON _s0.mid = _s6.merchant_id
ORDER BY
  coupons_per_merchant DESC
LIMIT 1
