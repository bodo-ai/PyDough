SELECT
  COUNT(*) AS num_transactions,
  CASE
    WHEN COUNT(*) > 0
    THEN COALESCE(SUM(wallet_transactions_daily.amount), 0)
    ELSE NULL
  END AS total_amount
FROM main.wallet_transactions_daily AS wallet_transactions_daily
JOIN main.users AS users
  ON users.country = 'US' AND users.uid = wallet_transactions_daily.sender_id
WHERE
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(wallet_transactions_daily.created_at, 'start of day'))
  ) AS INTEGER) <= 7
