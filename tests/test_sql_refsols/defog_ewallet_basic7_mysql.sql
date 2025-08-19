SELECT
  status,
  COUNT(*) AS count
FROM main.wallet_transactions_daily
GROUP BY
  1
ORDER BY
  count DESC
LIMIT 3
