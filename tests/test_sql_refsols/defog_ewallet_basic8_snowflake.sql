WITH _s1 AS (
  SELECT
    amount,
    coupon_id,
    txid
  FROM main.wallet_transactions_daily
)
SELECT
  ANY_VALUE(coupons.code) AS coupon_code,
  COUNT(_s1.txid) AS redemption_count,
  COALESCE(SUM(_s1.amount), 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
GROUP BY
  _s1.coupon_id
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
