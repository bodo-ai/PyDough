SELECT
  ANY_VALUE(merchants.name) AS name,
  COUNT(DISTINCT wallet_transactions_daily.coupon_id) / NULLIF(COUNT(DISTINCT wallet_transactions_daily.txid), 0) AS CPUR
FROM defog.ewallet.merchants AS merchants
JOIN defog.ewallet.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
