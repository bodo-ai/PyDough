SELECT
  status,
  COUNT(*) AS count
FROM main.wallet_transactions_daily
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
