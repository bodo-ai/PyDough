SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM mysql.broker.sbtransaction
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 3
