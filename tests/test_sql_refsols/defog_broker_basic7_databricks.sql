SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM defog.broker.sbtransaction
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 3
