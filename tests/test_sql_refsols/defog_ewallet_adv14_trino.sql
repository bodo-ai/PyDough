SELECT
  CAST(COALESCE(SUM(status = 'success'), 0) AS DOUBLE) / NULLIF(COUNT(*), 0) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  DATE_DIFF('MONTH', CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP) = 1
