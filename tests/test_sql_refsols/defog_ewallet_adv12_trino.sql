SELECT
  coupons.cid AS coupon_id,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_discount
FROM cassandra.defog.coupons AS coupons
LEFT JOIN postgres.main.wallet_transactions_daily AS wallet_transactions_daily
  ON coupons.cid = wallet_transactions_daily.coupon_id
WHERE
  coupons.merchant_id = 1
GROUP BY
  1
