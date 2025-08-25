SELECT
  sbtransaction.sbtxid AS transaction_id,
  HOUR(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS _expr0,
  MINUTE(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS _expr1,
  SECOND(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS _expr2
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  1 NULLS FIRST
