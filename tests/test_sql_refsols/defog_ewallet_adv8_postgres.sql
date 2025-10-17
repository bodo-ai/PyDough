SELECT
  MAX(merchants.mid) AS merchants_id,
  MAX(merchants.name) AS merchants_name,
  MAX(merchants.category) AS category,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(wallet_transactions_daily.amount), 0) DESC) AS mrr
FROM main.merchants AS merchants
JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.receiver_type = 1
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
