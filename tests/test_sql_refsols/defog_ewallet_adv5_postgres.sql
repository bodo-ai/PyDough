SELECT
  AVG(balance) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - CAST(updated_at AS TIMESTAMP)) / 86400 <= 7
