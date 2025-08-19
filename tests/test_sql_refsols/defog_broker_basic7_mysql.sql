SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM main.sbTransaction
GROUP BY
  1
ORDER BY
  num_transactions DESC
LIMIT 3
