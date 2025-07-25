SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM main.sbtransaction
GROUP BY
  sbtxstatus
ORDER BY
  num_transactions DESC NULLS LAST
LIMIT 3
