SELECT
  sbtxstatus AS status,
  COUNT(*) AS num_transactions
FROM main.sbtransaction
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
