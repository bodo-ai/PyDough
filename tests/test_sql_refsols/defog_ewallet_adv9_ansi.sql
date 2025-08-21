SELECT
  DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM main.wallet_transactions_daily
WHERE
  created_at < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  AND created_at >= DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -2, 'MONTH')
  AND sender_type = 0
GROUP BY
  1
