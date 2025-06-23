WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(wallet_transactions_daily.amount) AS sum_amount
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  JOIN main.users AS users
    ON users.country = 'US' AND users.uid = wallet_transactions_daily.sender_id
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(wallet_transactions_daily.created_at, 'start of day'))
    ) AS INTEGER) <= 7
)
SELECT
  n_rows AS num_transactions,
  CASE WHEN n_rows > 0 THEN COALESCE(sum_amount, 0) ELSE NULL END AS total_amount
FROM _t0
