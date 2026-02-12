SELECT
  AVG(balance) AS AMB
FROM ewallet.wallet_user_balance_daily
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), updated_at) <= 7
