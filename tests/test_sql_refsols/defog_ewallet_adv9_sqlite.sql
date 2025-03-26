SELECT
  DATE(wallet_transactions_daily.created_at, 'start of month') AS year_month,
  COUNT(DISTINCT wallet_transactions_daily.sender_id) AS active_users
FROM main.wallet_transactions_daily AS wallet_transactions_daily
WHERE
  wallet_transactions_daily.created_at < DATE('now', 'start of month')
  AND wallet_transactions_daily.created_at >= DATE('now', 'start of month', '-2 month')
  AND wallet_transactions_daily.sender_type = 0
GROUP BY
  DATE(wallet_transactions_daily.created_at, 'start of month')
