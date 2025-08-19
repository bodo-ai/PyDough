SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM main.sbtransaction
GROUP BY
  1
ORDER BY
  num_transactions DESC
LIMIT 3
