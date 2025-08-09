SELECT
  COALESCE(SUM(status = 'success'), 0) / COUNT(*) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  (
    (
      YEAR(CURRENT_TIMESTAMP()) - YEAR(created_at)
    ) * 12 + (
      MONTH(CURRENT_TIMESTAMP()) - MONTH(created_at)
    )
  ) = 1
