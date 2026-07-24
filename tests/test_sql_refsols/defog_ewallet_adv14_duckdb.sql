SELECT
  COALESCE(COUNT_IF(status = 'success'), 0) / NULLIF(COUNT(*), 0) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  DATE_DIFF(
    'MONTH',
    CAST(created_at AS TIMESTAMP),
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)
  ) = 1
