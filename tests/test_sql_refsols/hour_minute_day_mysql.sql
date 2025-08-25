SELECT
  sbtransaction.sbtxid COLLATE utf8mb4_bin AS transaction_id,
  HOUR(sbtransaction.sbtxdatetime) AS _expr0,
  MINUTE(sbtransaction.sbtxdatetime) AS _expr1,
  SECOND(sbtransaction.sbtxdatetime) AS _expr2
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  1
