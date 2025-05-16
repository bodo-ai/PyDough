SELECT
  DATE(wallet_transactions_daily.created_at, 'start of month') AS month,
  COUNT(DISTINCT wallet_transactions_daily.sender_id) AS active_users
FROM main.wallet_transactions_daily AS wallet_transactions_daily
JOIN main.users AS users
  ON users.status = 'active' AND users.uid = wallet_transactions_daily.sender_id
WHERE
  CAST(STRFTIME('%Y', wallet_transactions_daily.created_at) AS INTEGER) = 2023
  AND wallet_transactions_daily.sender_type = 0
  AND wallet_transactions_daily.status = 'success'
GROUP BY
  DATE(wallet_transactions_daily.created_at, 'start of month')
