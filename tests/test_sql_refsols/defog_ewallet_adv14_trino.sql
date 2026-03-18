SELECT
  CAST(COUNT_IF(status = 'success') AS DOUBLE) / NULLIF(COUNT(*), 0) AS _expr0
FROM postgres.wallet_transactions_daily
WHERE
  DATE_DIFF('MONTH', CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP) = 1
