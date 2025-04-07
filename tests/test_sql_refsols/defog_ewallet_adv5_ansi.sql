SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), updated_at, DAY) <= 7
