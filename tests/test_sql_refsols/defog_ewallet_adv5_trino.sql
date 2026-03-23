SELECT
  AVG(balance) AS AMB
FROM postgres.main.wallet_user_balance_daily
WHERE
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', updated_at) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) <= 7
