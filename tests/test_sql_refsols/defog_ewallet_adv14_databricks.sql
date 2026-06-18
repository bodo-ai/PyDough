SELECT
  COUNT_IF(status = 'success') / NULLIF(COUNT(*), 0) AS _expr0
FROM main.wallet_transactions_daily
WHERE
  (
    (
      YEAR(TO_DATE(CURRENT_TIMESTAMP())) - YEAR(TO_DATE(created_at))
    ) * 12 + MONTH(TO_DATE(CURRENT_TIMESTAMP())) - MONTH(TO_DATE(created_at))
  ) = 1
