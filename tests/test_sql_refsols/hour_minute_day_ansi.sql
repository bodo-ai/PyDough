SELECT
  sbtransaction.sbtxid AS transaction_id,
  EXTRACT(HOUR FROM sbtransaction.sbtxdatetime) AS _expr0,
  EXTRACT(MINUTE FROM sbtransaction.sbtxdatetime) AS _expr1,
  EXTRACT(SECOND FROM sbtransaction.sbtxdatetime) AS _expr2
FROM main.sbtransaction AS sbtransaction
LEFT JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
WHERE
  sbticker.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  sbtransaction.sbtxid
