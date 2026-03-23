SELECT
  CAST(COUNT_IF(status = 'success') AS DOUBLE) / NULLIF(COUNT(*), 0) AS _expr0
FROM postgres.main.wallet_transactions_daily
WHERE
  DATE_DIFF(
    'MONTH',
    CAST(DATE_TRUNC('MONTH', created_at) AS TIMESTAMP),
    CAST(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) = 1
