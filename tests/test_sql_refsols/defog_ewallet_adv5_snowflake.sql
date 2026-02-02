SELECT
  AVG(balance) AS AMB
FROM ewallet.wallet_user_balance_daily
WHERE
  DATEDIFF(
    DAY,
    CAST(updated_at AS DATETIME),
    CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
  ) <= 7
