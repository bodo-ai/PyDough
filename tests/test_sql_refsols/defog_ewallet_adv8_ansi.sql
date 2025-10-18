SELECT
  ANY_VALUE(merchants.mid) AS merchants_id,
  ANY_VALUE(merchants.name) AS merchants_name,
  ANY_VALUE(merchants.category) AS category,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(wallet_transactions_daily.amount), 0) DESC NULLS FIRST) AS mrr
FROM main.merchants AS merchants
JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON merchants.mid = wallet_transactions_daily.receiver_id
  AND wallet_transactions_daily.receiver_type = 1
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  wallet_transactions_daily.receiver_id
