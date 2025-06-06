SELECT
  sbtransaction.sbtxid AS transaction_id,
  EXTRACT(HOUR FROM sbtransaction.sbtxdatetime) AS _expr0,
  EXTRACT(MINUTE FROM sbtransaction.sbtxdatetime) AS _expr1,
  EXTRACT(SECOND FROM sbtransaction.sbtxdatetime) AS _expr2
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  transaction_id
