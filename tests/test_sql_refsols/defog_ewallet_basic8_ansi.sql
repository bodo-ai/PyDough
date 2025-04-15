WITH _s1 AS (
  SELECT
    COUNT(txid) AS agg_0,
    SUM(amount) AS agg_1,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  coupons.code AS coupon_code,
  COALESCE(_s1.agg_0, 0) AS redemption_count,
  COALESCE(_s1.agg_1, 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
ORDER BY
  redemption_count DESC
LIMIT 3
