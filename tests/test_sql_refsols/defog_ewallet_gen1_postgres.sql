SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    wallet_merchant_balance_daily.balance) AS _expr0
FROM main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
JOIN main.merchants AS merchants
  ON LOWER(merchants.category) LIKE '%retail%'
  AND merchants.mid = wallet_merchant_balance_daily.merchant_id
  AND merchants.status = 'active'
WHERE
  DATE_TRUNC('DAY', CAST(wallet_merchant_balance_daily.updated_at AS TIMESTAMP)) = DATE_TRUNC('DAY', CURRENT_TIMESTAMP)
