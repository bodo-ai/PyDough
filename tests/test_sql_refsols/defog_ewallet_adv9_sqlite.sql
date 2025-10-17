SELECT
  DATE(created_at, 'start of month') AS year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM main.wallet_transactions_daily
WHERE
  created_at < DATE('now', 'start of month')
  AND created_at >= DATE('now', 'start of month', '-2 month')
  AND sender_type = 0
GROUP BY
  1
