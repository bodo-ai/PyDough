SELECT
  sbTransaction.sbTxId COLLATE utf8mb4_bin AS transaction_id,
  HOUR(sbTransaction.sbTxDateTime) AS _expr0,
  MINUTE(sbTransaction.sbTxDateTime) AS _expr1,
  SECOND(sbTransaction.sbTxDateTime) AS _expr2
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbTickerId = sbTransaction.sbTxTickerId
  AND sbTicker.sbTickerSymbol IN ('AAPL', 'GOOGL', 'NFLX')
WHERE
  EXTRACT(YEAR FROM CAST(sbTransaction.sbTxDateTime AS DATETIME)) = 2023
ORDER BY
  1
