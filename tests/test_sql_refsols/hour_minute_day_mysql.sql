SELECT
  sbTransaction.sbtxid COLLATE utf8mb4_bin AS transaction_id,
  HOUR(sbTransaction.sbtxdatetime) AS _expr0,
  MINUTE(sbTransaction.sbtxdatetime) AS _expr1,
  SECOND(sbTransaction.sbtxdatetime) AS _expr2
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  AND sbTicker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
WHERE
  EXTRACT(YEAR FROM CAST(sbTransaction.sbtxdatetime AS DATETIME)) = 2023
ORDER BY
  1
