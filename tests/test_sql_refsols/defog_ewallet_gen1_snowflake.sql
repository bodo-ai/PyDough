SELECT
  MEDIAN(wallet_merchant_balance_daily.balance) AS _expr0
FROM main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
JOIN main.merchants AS merchants
  ON CONTAINS(LOWER(merchants.category), 'retail')
  AND merchants.mid = wallet_merchant_balance_daily.merchant_id
  AND merchants.status = 'active'
WHERE
  DATE_TRUNC('DAY', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) = DATE_TRUNC('DAY', CAST(wallet_merchant_balance_daily.updated_at AS TIMESTAMP))
