SELECT
  MAX(coupons.code) AS coupon_code,
  COUNT(wallet_transactions_daily.txid) AS redemption_count,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON coupons.cid = wallet_transactions_daily.coupon_id
GROUP BY
  coupons.cid
ORDER BY
  2 DESC
LIMIT 3
