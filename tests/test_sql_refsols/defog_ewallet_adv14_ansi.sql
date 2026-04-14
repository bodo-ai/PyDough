SELECT
  COALESCE(SUM(status = 'success'), 0) / NULLIF(COUNT(*), 0) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), CAST(created_at AS DATETIME), MONTH) = 1
