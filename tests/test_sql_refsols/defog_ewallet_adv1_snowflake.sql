SELECT
  ANY_VALUE(merchants.name) AS name,
  COUNT(DISTINCT wallet_transactions_daily.coupon_id) / COUNT(DISTINCT wallet_transactions_daily.txid) AS CPUR
FROM ewallet.merchants AS merchants
JOIN ewallet.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
