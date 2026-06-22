SELECT
  TRUNC(CAST(created_at AS TIMESTAMP), 'MONTH') AS year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM main.wallet_transactions_daily
WHERE
  created_at < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
  AND created_at >= ADD_MONTHS(TRUNC(CURRENT_TIMESTAMP(), 'MONTH'), -2)
  AND sender_type = 0
GROUP BY
  1
