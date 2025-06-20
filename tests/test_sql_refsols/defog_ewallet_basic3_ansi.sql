SELECT
  merchants.mid AS merchant
FROM main.merchants AS merchants
JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.receiver_type = 1
