SELECT
  AVG(CAST(balance AS DECIMAL)) AS AMB
FROM main.wallet_user_balance_daily
WHERE
  (
    CAST(CURRENT_TIMESTAMP AS DATE) - CAST(updated_at AS DATE)
  ) <= 7
