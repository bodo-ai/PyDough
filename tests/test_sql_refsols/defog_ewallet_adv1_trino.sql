SELECT
  ARBITRARY(merchants.name) AS name,
  CAST(COUNT(DISTINCT wallet_transactions_daily.coupon_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT wallet_transactions_daily.txid), 0) AS CPUR
FROM postgres.main.merchants AS merchants
JOIN postgres.main.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
