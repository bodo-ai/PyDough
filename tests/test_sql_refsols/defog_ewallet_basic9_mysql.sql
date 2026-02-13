SELECT
  users.country,
  COUNT(DISTINCT wallet_transactions_daily.sender_id) AS user_count,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_amount
FROM main.wallet_transactions_daily AS wallet_transactions_daily
LEFT JOIN main.users AS users
  ON users.uid = wallet_transactions_daily.sender_id
WHERE
  wallet_transactions_daily.sender_type = 0
GROUP BY
  1
ORDER BY
  3 DESC
LIMIT 5
