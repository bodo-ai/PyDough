SELECT
  CAST(COALESCE(SUM(status = 'success'), 0) AS REAL) / COUNT(*) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  (
    (
      CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', created_at) AS INTEGER)
    ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', created_at) AS INTEGER)
  ) = 1
