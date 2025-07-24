SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM main.sbTransaction
GROUP BY
  sbtxstatus
ORDER BY
  num_transactions DESC
LIMIT 3
