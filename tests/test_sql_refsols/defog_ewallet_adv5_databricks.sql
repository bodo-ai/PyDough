SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  DATEDIFF(DAY, CAST(updated_at AS DATE), CAST(CURRENT_TIMESTAMP() AS DATE)) <= 7
