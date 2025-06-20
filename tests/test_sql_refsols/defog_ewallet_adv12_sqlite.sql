WITH _s3 AS (
  SELECT
    SUM(amount) AS agg_0,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  _s0.cid AS coupon_id,
  COALESCE(_s3.agg_0, 0) AS total_discount
FROM main.coupons AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.cid = _s3.coupon_id
WHERE
  _s0.merchant_id = '1'
