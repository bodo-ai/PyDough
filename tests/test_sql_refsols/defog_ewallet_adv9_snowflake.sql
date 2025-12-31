SELECT
  DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM main.wallet_transactions_daily
WHERE
  created_at < DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
  AND created_at >= DATEADD(
    MONTH,
    -2,
    DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
  )
  AND sender_type = 0
GROUP BY
  1
