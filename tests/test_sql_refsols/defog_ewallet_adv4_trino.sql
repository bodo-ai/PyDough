SELECT
  COUNT(*) AS num_transactions,
  CASE
    WHEN COUNT(*) <> 0
    THEN COALESCE(SUM(wallet_transactions_daily.amount), 0)
    ELSE NULL
  END AS total_amount
FROM postgres.main.wallet_transactions_daily AS wallet_transactions_daily
JOIN postgres.main.users AS users
  ON users.country = 'US' AND users.uid = wallet_transactions_daily.sender_id
WHERE
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', wallet_transactions_daily.created_at) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) <= 7
