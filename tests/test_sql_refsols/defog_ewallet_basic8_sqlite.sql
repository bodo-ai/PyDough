SELECT
  MAX(coupons.code) AS coupon_code,
  COALESCE(
    CASE WHEN COUNT(*) <> 0 THEN COUNT(wallet_transactions_daily.txid) ELSE NULL END,
    0
  ) AS redemption_count,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON coupons.cid = wallet_transactions_daily.coupon_id
GROUP BY
  coupons.cid
ORDER BY
  2 DESC
LIMIT 3
