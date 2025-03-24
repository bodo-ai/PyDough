SELECT
  AVG(balance) AS AMB
FROM (
  SELECT
    balance
  FROM (
    SELECT
      balance,
      updated_at
    FROM main.wallet_user_balance_daily
  )
  WHERE
    CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(updated_at, 'start of day'))) AS INTEGER) <= 7
)
