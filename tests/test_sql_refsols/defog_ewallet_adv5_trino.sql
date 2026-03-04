SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  DATE_DIFF('DAY', CAST(updated_at AS TIMESTAMP), CURRENT_TIMESTAMP) <= 7
