SELECT
  ANY_VALUE(merchants.name) AS name,
  COUNT(DISTINCT wallet_transactions_daily.coupon_id) / COUNT(DISTINCT wallet_transactions_daily.txid) AS CPUR
FROM main.merchants AS merchants
JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
