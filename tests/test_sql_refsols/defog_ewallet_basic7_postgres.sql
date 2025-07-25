SELECT
  status,
  COUNT(*) AS count
FROM main.wallet_transactions_daily
GROUP BY
  status
ORDER BY
  count DESC NULLS LAST
LIMIT 3
