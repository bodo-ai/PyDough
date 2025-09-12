SELECT
  sbtransaction.sbtxid AS transaction_id,
  EXTRACT(HOUR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) AS _expr0,
  EXTRACT(MINUTE FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) AS _expr1,
  EXTRACT(SECOND FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) AS _expr2
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
WHERE
  EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 2023
ORDER BY
  1
