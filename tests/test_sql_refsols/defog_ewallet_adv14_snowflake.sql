SELECT
  COUNT_IF(status = 'success') / COUNT(*) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  DATEDIFF(
    MONTH,
    CAST(created_at AS DATETIME),
    CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
  ) = 1
