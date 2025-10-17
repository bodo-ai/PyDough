SELECT
  wallet_transactions_daily.sender_id AS user_id,
  COUNT(*) AS total_transactions
FROM main.users AS users
JOIN main.wallet_transactions_daily AS wallet_transactions_daily
  ON users.uid = wallet_transactions_daily.sender_id
  AND wallet_transactions_daily.sender_type = 0
GROUP BY
  1
