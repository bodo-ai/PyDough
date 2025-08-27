SELECT
  COALESCE(COUNT_IF(status = 'success'), 0) / COUNT(*) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  DATEDIFF(MONTH, CAST(created_at AS DATETIME), CURRENT_TIMESTAMP()) = 1
