SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  DATE_DIFF(
    'DAY',
    CAST(updated_at AS TIMESTAMP),
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)
  ) <= 7
