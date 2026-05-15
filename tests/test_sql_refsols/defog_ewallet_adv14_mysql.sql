SELECT
  COALESCE(SUM(status = 'success'), 0) / NULLIF(COUNT(*), 0) AS _expr0
FROM ewallet.wallet_transactions_daily
WHERE
  (
    (
      YEAR(CURRENT_TIMESTAMP()) - YEAR(created_at)
    ) * 12 + (
      MONTH(CURRENT_TIMESTAMP()) - MONTH(created_at)
    )
  ) = 1
