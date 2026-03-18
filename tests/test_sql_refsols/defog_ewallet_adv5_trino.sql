SELECT
  AVG(balance) AS AMB
FROM postgres.wallet_user_balance_daily
WHERE
  DATE_DIFF('DAY', CAST(updated_at AS TIMESTAMP), CURRENT_TIMESTAMP) <= 7
