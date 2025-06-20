SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), CAST(updated_at AS DATETIME), DAY) <= 7
