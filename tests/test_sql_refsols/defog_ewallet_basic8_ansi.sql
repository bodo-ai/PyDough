WITH _s3 AS (
  SELECT
    COUNT(txid) AS agg_0,
    SUM(amount) AS agg_1,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  _s0.code AS coupon_code,
  COALESCE(_s3.agg_0, 0) AS redemption_count,
  COALESCE(_s3.agg_1, 0) AS total_discount
FROM main.coupons AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.cid = _s3.coupon_id
ORDER BY
  redemption_count DESC
LIMIT 3
